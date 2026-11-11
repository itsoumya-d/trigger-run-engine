/**
 * Core RunEngine orchestrating durable runs, idempotency keys, waitpoints, retries & DLQ replay.
 */
import { createHash } from "node:crypto";
import { RedisLeaseBroker } from "./leaseQueue.js";
import { formatTraceparent, OTelTracer, RecordedSpan } from "../telemetry/otel.js";

export type RunStatus =
  | "PENDING"
  | "EXECUTING"
  | "WAITING_FOR_TOKEN"
  | "WAITING_FOR_DURATION"
  | "COMPLETED"
  | "FAILED"
  | "DEAD_LETTERED";

export interface TaskRunRecord {
  id: string;
  orgId: string;
  envSlug: string;
  taskId: string;
  idempotencyKey?: string;
  payloadSha256: string;
  payload: unknown;
  status: RunStatus;
  attemptCount: number;
  maxAttempts: number;
  concurrencyKey: string;
  traceparent: string;
  output?: unknown;
  lastError?: string;
}

export interface WaitpointRecord {
  token: string;
  runId: string;
  kind: "TOKEN" | "DURATION";
  resumeAfterMs?: number;
  resolvedPayload?: unknown;
  resolved: boolean;
}

export interface TriggerRequest {
  orgId: string;
  envSlug: string;
  taskId: string;
  payload: unknown;
  idempotencyKey?: string;
  maxAttempts?: number;
  concurrencyKey?: string;
  parentTraceparent?: string;
}

export interface ClaimedExecution {
  run: TaskRunRecord;
  attemptId: string;
  attemptNumber: number;
  leaseToken: string;
  span: RecordedSpan;
}

export class IdempotencyConflictError extends Error {
  constructor(key: string) {
    super(`Idempotency key "${key}" already bound to a different payload digest`);
  }
}

function stableDigest(value: unknown): string {
  const canonical = JSON.stringify(value, Object.keys(value as Record<string, unknown>).sort());
  return createHash("sha256").update(canonical).digest("hex");
}

export class RunEngine {
  readonly runs = new Map<string, TaskRunRecord>();
  readonly idempotencyIndex = new Map<string, string>();
  readonly waitpoints = new Map<string, WaitpointRecord>();
  readonly deadLetters: TaskRunRecord[] = [];
  readonly queue = new RedisLeaseBroker();
  readonly tracer = new OTelTracer();

  private idSeq = 1;
  private leaseSeq = 1;

  trigger(req: TriggerRequest, nowMs: number): { run: TaskRunRecord; deduplicated: boolean } {
    const digest = stableDigest(req.payload ?? {});
    const scopedIdem = req.idempotencyKey
      ? `${req.orgId}:${req.envSlug}:${req.taskId}:${req.idempotencyKey}`
      : null;

    if (scopedIdem) {
      const existingId = this.idempotencyIndex.get(scopedIdem);
      if (existingId) {
        const existing = this.runs.get(existingId)!;
        if (existing.payloadSha256 !== digest) {
          throw new IdempotencyConflictError(req.idempotencyKey!);
        }
        return { run: existing, deduplicated: true };
      }
    }

    const runId = `run_${String(this.idSeq++).padStart(6, "0")}`;
    const span = req.parentTraceparent
      ? this.tracer.startChildSpan(`task.trigger:${req.taskId}`, req.parentTraceparent, nowMs, {
          "task.id": req.taskId,
          "org.id": req.orgId,
        })
      : this.tracer.startRootSpan(`task.trigger:${req.taskId}`, nowMs, {
          "task.id": req.taskId,
          "org.id": req.orgId,
        });
    this.tracer.finishSpan(span, nowMs, "OK");

    const run: TaskRunRecord = {
      id: runId,
      orgId: req.orgId,
      envSlug: req.envSlug,
      taskId: req.taskId,
      idempotencyKey: req.idempotencyKey,
      payloadSha256: digest,
      payload: req.payload,
      status: "PENDING",
      attemptCount: 0,
      maxAttempts: req.maxAttempts ?? 3,
      concurrencyKey: req.concurrencyKey ?? `${req.orgId}:${req.taskId}`,
      traceparent: formatTraceparent(span.traceId, span.spanId),
    };

    this.runs.set(runId, run);
    if (scopedIdem) this.idempotencyIndex.set(scopedIdem, runId);

    this.enqueueAttempt(run, nowMs);
    return { run, deduplicated: false };
  }

  private enqueueAttempt(run: TaskRunRecord, availableAtMs: number): void {
    const nextAttempt = run.attemptCount + 1;
    this.queue.enqueue({
      runId: run.id,
      attemptId: `${run.id}_att_${nextAttempt}`,
      attemptNumber: nextAttempt,
      concurrencyKey: run.concurrencyKey,
      availableAtMs,
    });
  }

  claimNextAttempt(nowMs: number, leaseDurationMs = 30_000, maxConcurrency = 4): ClaimedExecution | null {
    const leaseToken = `lease_${this.leaseSeq++}`;
    const lease = this.queue.claimNext(nowMs, leaseDurationMs, maxConcurrency, leaseToken);
    if (!lease) return null;

    const run = this.runs.get(lease.runId);
    if (!run) return null;

    run.status = "EXECUTING";
    run.attemptCount = Math.max(run.attemptCount, lease.attemptNumber);

    const span = this.tracer.startChildSpan(`attempt.exec:${run.taskId}`, run.traceparent, nowMs, {
      "run.id": run.id,
      "attempt.number": lease.attemptNumber,
    });

    return {
      run,
      attemptId: lease.attemptId,
      attemptNumber: lease.attemptNumber,
      leaseToken: lease.leaseToken,
      span,
    };
  }

  completeAttempt(claimed: ClaimedExecution, output: unknown, nowMs: number): boolean {
    if (!this.queue.ack(claimed.attemptId, claimed.leaseToken)) {
      this.tracer.finishSpan(claimed.span, nowMs, "ERROR", "Fencing token stale/lost");
      return false;
    }
    claimed.run.status = "COMPLETED";
    claimed.run.output = output;
    this.tracer.finishSpan(claimed.span, nowMs, "OK");
    return true;
  }

  failAttempt(claimed: ClaimedExecution, errorMsg: string, nowMs: number, baseBackoffMs = 1_000): RunStatus {
    if (!this.queue.ack(claimed.attemptId, claimed.leaseToken)) {
      this.tracer.finishSpan(claimed.span, nowMs, "ERROR", "Stale lease cannot fail run");
      return claimed.run.status;
    }
    this.tracer.finishSpan(claimed.span, nowMs, "ERROR", errorMsg);
    claimed.run.lastError = errorMsg;

    if (claimed.run.attemptCount >= claimed.run.maxAttempts) {
      claimed.run.status = "DEAD_LETTERED";
      this.deadLetters.push(claimed.run);
      return "DEAD_LETTERED";
    }

    claimed.run.status = "PENDING";
    const backoff = baseBackoffMs * Math.pow(2, claimed.run.attemptCount - 1);
    this.enqueueAttempt(claimed.run, nowMs + backoff);
    return "PENDING";
  }

  suspendForToken(claimed: ClaimedExecution, token: string, nowMs: number): WaitpointRecord {
    this.queue.release(claimed.attemptId, claimed.leaseToken);
    claimed.run.status = "WAITING_FOR_TOKEN";
    this.tracer.finishSpan(claimed.span, nowMs, "OK");
    const wp: WaitpointRecord = {
      token,
      runId: claimed.run.id,
      kind: "TOKEN",
      resolved: false,
    };
    this.waitpoints.set(token, wp);
    return wp;
  }

  resolveWaitpoint(token: string, payload: unknown, nowMs: number): TaskRunRecord | null {
    const wp = this.waitpoints.get(token);
    if (!wp || wp.resolved) return null;
    wp.resolved = true;
    wp.resolvedPayload = payload;
    const run = this.runs.get(wp.runId)!;
    run.status = "PENDING";
    run.payload = { ...(run.payload as Record<string, unknown>), resumedWith: payload };
    this.enqueueAttempt(run, nowMs);
    return run;
  }

  replayDeadLetter(runId: string, nowMs: number): TaskRunRecord | null {
    const idx = this.deadLetters.findIndex((r) => r.id === runId);
    if (idx === -1) return null;
    const [run] = this.deadLetters.splice(idx, 1);
    if (!run) return null;
    run.status = "PENDING";
    run.maxAttempts += 1;
    this.enqueueAttempt(run, nowMs);
    return run;
  }
}
