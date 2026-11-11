# trigger-run-engine

Durable background task execution engine written in strict TypeScript (Node.js) featuring PostgreSQL schema migrations, Redis-semantics atomic lease queues with fencing tokens, waitpoint pause/resume checkpoints, exponential retry + dead-letter replay, W3C OpenTelemetry `traceparent` span linking, and a Zod-validated Remix server bridge.

## Architecture

- [`sql/001_run_engine.sql`](sql/001_run_engine.sql): PostgreSQL schema (`task_runs`, `task_attempts`, `waitpoints`) with unique idempotency partial index and overdue duration index.
- [`src/engine/leaseQueue.ts`](src/engine/leaseQueue.ts): Atomic `claimNext` with concurrency-key throttling, lease heartbeat extension, split-brain fencing token checks, and crash reclamation.
- [`src/engine/runEngine.ts`](src/engine/runEngine.ts): `RunEngine` state machine (`PENDING -> EXECUTING -> WAITING_FOR_TOKEN -> COMPLETED | DEAD_LETTERED`) and DLQ replay.
- [`src/telemetry/otel.ts`](src/telemetry/otel.ts): W3C `traceparent` parser/propagator and Node `perf_hooks` event-loop p95 delay sampler.
- [`src/remix/taskBridge.ts`](src/remix/taskBridge.ts): Remix action/loader boundary + HMAC-SHA256 webhook verification.

## Verification

```bash
npm ci
npm run build
npm test
```
