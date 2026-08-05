CREATE TABLE IF NOT EXISTS workers (
  id                 UUID PRIMARY KEY,
  version            TEXT NOT NULL,
  max_parallel       INT  NOT NULL,
  registered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  status             TEXT NOT NULL CHECK (status IN ('Online','Draining','Offline'))
);
CREATE INDEX IF NOT EXISTS ix_workers_heartbeat ON workers(last_heartbeat_at);

CREATE TABLE IF NOT EXISTS job_batches (
  id           UUID PRIMARY KEY,
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  submitter    TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
  id                   UUID PRIMARY KEY,
  batch_id             UUID NOT NULL REFERENCES job_batches(id),
  job_type             TEXT NOT NULL,
  payload              JSONB NOT NULL,
  priority             SMALLINT NOT NULL DEFAULT 5,
  status               TEXT NOT NULL CHECK (status IN
                         ('Pending','Assigned','Processing',
                          'Success','Failed','DeadLettered','Cancelled')),
  attempts             INT NOT NULL DEFAULT 0,
  max_attempts         INT NOT NULL DEFAULT 3,
  assigned_worker_id   UUID REFERENCES workers(id),
  lease_expires_at     TIMESTAMPTZ,
  estimated_runtime_ms INT,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at           TIMESTAMPTZ,
  finished_at          TIMESTAMPTZ,
  result               JSONB,
  error                TEXT
);

CREATE INDEX IF NOT EXISTS ix_tasks_pending_priority
  ON tasks(priority DESC, created_at ASC)
  WHERE status = 'Pending';

CREATE INDEX IF NOT EXISTS ix_tasks_lease
  ON tasks(lease_expires_at)
  WHERE status IN ('Assigned','Processing');

CREATE INDEX IF NOT EXISTS ix_tasks_batch ON tasks(batch_id);

CREATE TABLE IF NOT EXISTS task_events (
  id           BIGSERIAL PRIMARY KEY,
  task_id      UUID NOT NULL REFERENCES tasks(id),
  from_status  TEXT,
  to_status    TEXT NOT NULL,
  worker_id    UUID,
  at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  detail       JSONB
);
CREATE INDEX IF NOT EXISTS ix_task_events_task ON task_events(task_id, at);
