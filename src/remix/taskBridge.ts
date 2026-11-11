/**
 * Typed Remix server loader/action bridge + HMAC-SHA256 webhook verifier.
 */
import { createHmac, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import { RunEngine } from "../engine/runEngine.js";

const TriggerActionSchema = z.object({
  orgId: z.string().min(1),
  envSlug: z.string().min(1),
  taskId: z.string().min(1),
  idempotencyKey: z.string().min(1).optional(),
  payload: z.record(z.unknown()),
});

export function signWebhookBody(secret: string, body: string): string {
  return createHmac("sha256", secret).update(body).digest("hex");
}

export function verifyWebhookSignature(secret: string, body: string, providedHex: string): boolean {
  const expected = Buffer.from(signWebhookBody(secret, body), "hex");
  const actual = Buffer.from(providedHex.trim(), "hex");
  if (expected.length !== actual.length) return false;
  return timingSafeEqual(expected, actual);
}

export function handleRemixTriggerAction(
  engine: RunEngine,
  body: unknown,
  parentTraceparent: string | undefined,
  nowMs: number,
): { status: number; runId?: string; deduplicated?: boolean; error?: string } {
  const parsed = TriggerActionSchema.safeParse(body);
  if (!parsed.success) {
    return { status: 400, error: parsed.error.issues[0]?.message ?? "Invalid request" };
  }
  const { run, deduplicated } = engine.trigger(
    { ...parsed.data, parentTraceparent },
    nowMs,
  );
  return { status: deduplicated ? 200 : 202, runId: run.id, deduplicated };
}

export function loadRemixRunSummary(engine: RunEngine, runId: string) {
  const run = engine.runs.get(runId);
  if (!run) return null;
  return {
    id: run.id,
    taskId: run.taskId,
    status: run.status,
    attempts: run.attemptCount,
    traceparent: run.traceparent,
    output: run.output ?? null,
    lastError: run.lastError ?? null,
  };
}
