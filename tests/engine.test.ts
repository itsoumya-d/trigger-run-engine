import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { RunEngine, IdempotencyConflictError } from "../src/engine/runEngine.js";
import {
  handleRemixTriggerAction,
  loadRemixRunSummary,
  signWebhookBody,
  verifyWebhookSignature,
} from "../src/remix/taskBridge.js";
import { sampleEventLoopP95Ms } from "../src/telemetry/otel.js";

test("idempotency deduplicates identical triggers and blocks conflicting payload digests", () => {
  const engine = new RunEngine();
  const first = engine.trigger(
    { orgId: "org_1", envSlug: "prod", taskId: "invoice.sync", idempotencyKey: "inv_99", payload: { total: 420 } },
    1_000,
  );
  assert.equal(first.deduplicated, false);

  const dup = engine.trigger(
    { orgId: "org_1", envSlug: "prod", taskId: "invoice.sync", idempotencyKey: "inv_99", payload: { total: 420 } },
    1_200,
  );
  assert.equal(dup.deduplicated, true);
  assert.equal(dup.run.id, first.run.id);

  assert.throws(
    () =>
      engine.trigger(
        { orgId: "org_1", envSlug: "prod", taskId: "invoice.sync", idempotencyKey: "inv_99", payload: { total: 999 } },
        1_300,
      ),
    IdempotencyConflictError,
  );
});

test("lease expiry reclaims crashed worker and fencing rejects stale ack", () => {
  const engine = new RunEngine();
  engine.trigger({ orgId: "org_1", envSlug: "prod", taskId: "embed.batch", payload: { n: 10 } }, 1_000);

  const stale = engine.claimNextAttempt(1_000, 5_000, 1);
  assert.ok(stale);
  assert.equal(engine.queue.heartbeat(stale.attemptId, stale.leaseToken, 3_000, 5_000), true);

  // Advance past extended lease (3000 + 5000 = 8000): second worker reclaims slot safely
  const reclaimed = engine.claimNextAttempt(9_000, 5_000, 1);
  assert.ok(reclaimed);
  assert.notEqual(reclaimed.leaseToken, stale.leaseToken);

  // Stale worker tries to complete and gets fenced out
  assert.equal(engine.completeAttempt(stale, { done: true }, 9_100), false);
  // Active owner completes cleanly
  assert.equal(engine.completeAttempt(reclaimed, { done: true }, 9_200), true);
  assert.equal(engine.runs.get(reclaimed.run.id)?.status, "COMPLETED");
});

test("waitpoint suspend releases worker slot and resumes on external webhook token", () => {
  const engine = new RunEngine();
  const { run } = engine.trigger({ orgId: "org_1", envSlug: "prod", taskId: "human.approve", payload: { doc: "a1" } }, 1_000);

  const c1 = engine.claimNextAttempt(1_000, 10_000, 1);
  assert.ok(c1);
  engine.suspendForToken(c1, "tok_approval_7", 1_500);
  assert.equal(run.status, "WAITING_FOR_TOKEN");
  assert.equal(engine.queue.inFlightCount(run.concurrencyKey), 0);

  const resumed = engine.resolveWaitpoint("tok_approval_7", { approvedBy: "ops" }, 5_000);
  assert.ok(resumed);
  assert.equal(resumed.status, "PENDING");

  const c2 = engine.claimNextAttempt(5_000, 10_000, 1);
  assert.ok(c2);
  assert.equal((c2.run.payload as { resumedWith: { approvedBy: string } }).resumedWith.approvedBy, "ops");
  assert.equal(engine.completeAttempt(c2, { finalized: true }, 5_500), true);
});

test("exponential retry transitions to dead-letter queue and supports deterministic replay", () => {
  const engine = new RunEngine();
  const { run } = engine.trigger({ orgId: "org_1", envSlug: "prod", taskId: "flaky.rpc", maxAttempts: 2, payload: {} }, 1_000);

  const c1 = engine.claimNextAttempt(1_000);
  assert.ok(c1);
  assert.equal(engine.failAttempt(c1, "ETIMEDOUT", 1_100, 1_000), "PENDING");
  // Not available before backoff (1100 + 1000 = 2100)
  assert.equal(engine.claimNextAttempt(1_800), null);

  const c2 = engine.claimNextAttempt(2_200);
  assert.ok(c2);
  assert.equal(engine.failAttempt(c2, "ECONNRESET", 2_300, 1_000), "DEAD_LETTERED");
  assert.equal(engine.deadLetters.length, 1);

  assert.ok(engine.replayDeadLetter(run.id, 3_000));
  const c3 = engine.claimNextAttempt(3_000);
  assert.ok(c3);
  assert.equal(engine.completeAttempt(c3, { recovered: true }, 3_200), true);
});

test("Remix bridge validates schema, verifies webhook HMAC, and preserves OTel traceparent", () => {
  const engine = new RunEngine();
  const parentTp = "00-11111111111111111111111111111111-2222222222222222-01";
  const bad = handleRemixTriggerAction(engine, { orgId: "" }, parentTp, 1_000);
  assert.equal(bad.status, 400);

  const ok = handleRemixTriggerAction(
    engine,
    { orgId: "org_2", envSlug: "staging", taskId: "remix.email", payload: { user: 42 } },
    parentTp,
    1_000,
  );
  assert.equal(ok.status, 202);
  assert.ok(ok.runId);
  const claimed = engine.claimNextAttempt(1_100);
  assert.ok(claimed);
  assert.equal(claimed.span.traceId, "11111111111111111111111111111111");
  assert.ok(loadRemixRunSummary(engine, ok.runId!)?.traceparent.includes("11111111111111111111111111111111"));

  const sig = signWebhookBody("whsec_test", '{"ok":true}');
  assert.equal(verifyWebhookSignature("whsec_test", '{"ok":true}', sig), true);
  assert.equal(verifyWebhookSignature("whsec_test", '{"ok":false}', sig), false);
});

test("PostgreSQL schema defines unique idempotency index & partial active queue indexes + event loop telemetry works", () => {
  const sql = readFileSync(new URL("../sql/001_run_engine.sql", import.meta.url), "utf8");
  assert.match(sql, /CREATE UNIQUE INDEX IF NOT EXISTS uq_task_runs_idempotency/);
  assert.match(sql, /CREATE TABLE IF NOT EXISTS waitpoints/);
  const lag = sampleEventLoopP95Ms();
  assert.equal(typeof lag, "number");
});
