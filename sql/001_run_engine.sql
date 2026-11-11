-- Trigger Run Engine PostgreSQL Schema (Zero-Downtime Append Migrations)
-- Models durable task runs, execution attempts, lease fences, waitpoints, and DLQ triage.

CREATE TYPE run_status AS ENUM (
  'PENDING',
  'EXECUTING',
  'WAITING_FOR_TOKEN',
  'WAITING_FOR_DURATION',
  'COMPLETED',
  'FAILED',
  'DEAD_LETTERED'
);

CREATE TABLE IF NOT EXISTS task_runs (
  id UUID PRIMARY KEY,
  org_id TEXT NOT NULL,
  env_slug TEXT NOT NULL,
  task_id TEXT NOT NULL,
  idempotency_key TEXT,
  payload_sha256 TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  status run_status NOT NULL DEFAULT 'PENDING',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  traceparent TEXT NOT NULL,
  concurrency_key TEXT,
  output_json JSONB,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_task_runs_idempotency
  ON task_runs (org_id, env_slug, task_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_task_runs_active_org
  ON task_runs (org_id, env_slug, status, created_at DESC)
  WHERE status IN ('PENDING', 'EXECUTING', 'WAITING_FOR_TOKEN', 'WAITING_FOR_DURATION');

CREATE TABLE IF NOT EXISTS task_attempts (
  id UUID PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  lease_token TEXT NOT NULL,
  lease_expires_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  exit_code INTEGER,
  heartbeat_lag_ms REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_task_attempts_run_num
  ON task_attempts (run_id, attempt_number);

CREATE TABLE IF NOT EXISTS waitpoints (
  token TEXT PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('TOKEN', 'DURATION')),
  resume_after TIMESTAMPTZ,
  resolved_payload JSONB,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_waitpoints_due
  ON waitpoints (resume_after)
  WHERE kind = 'DURATION' AND resolved_at IS NULL;
