# Distributed Task Execution & Telemetry Engine — Design Document

**Version:** 0.1 (draft)
**Author:** Akshaj
**Date:** 2026-08-05
**Status:** Design — pre-implementation

---

## 1. Overview

A local-first distributed execution engine. A **C# / ASP.NET Core orchestrator** accepts batches of compute jobs over REST, persists their lifecycle in **PostgreSQL**, and dispatches them to a pool of **C++ worker nodes** over a persistent **gRPC bidirectional stream**. Workers run each job in an isolated worker thread, stream **OpenTelemetry** metrics back to an OTel Collector → Prometheus → **Grafana** dashboard, and return final results to the orchestrator for durable storage. The full cluster spins up locally with a single `docker compose up`.

### 1.1 Goals

1. **End-to-end correctness.** Every submitted job reaches a terminal state (`Success`, `Failed`, or `Cancelled`) exactly once, even across worker crashes.
2. **Observable performance.** Per-task CPU time, RSS delta, wall-clock latency, and queue-wait visible in Grafana within ~1 s of completion.
3. **Language boundary showcase.** Managed C# for I/O + control plane; unmanaged C++ for the compute plane. Interop is deliberate (gRPC, not P/Invoke).
4. **Portable local deployment.** Any machine with Docker Desktop should run the whole cluster with one command.
5. **Realistic workload.** Ships with at least two non-trivial job types (e.g. graph BFS/PageRank, string suffix-array construction) — not toy `sleep()` calls.

### 1.2 Non-goals

- Multi-datacenter or cloud deployment.
- Byzantine fault tolerance / consensus. A single orchestrator is a deliberate SPOF for v1.
- Authentication of end users. The REST API is trusted-network only.
- Hot code reloading of job types. New job types require a worker rebuild.
- Guaranteed exactly-once *side effects* — only exactly-once result recording.

### 1.3 Rough capacity target (for design sanity)

- ~200 concurrent workers × 8 threads each = ~1,600 in-flight tasks.
- Sustained submission rate: ~500 jobs/sec.
- Individual job runtime: 10 ms – 30 s.
- Telemetry cardinality: ≤ 50 distinct label combinations (worker_id × job_type × status).

These numbers drive schema, queue, and metric choices — not a production SLO.

---

## 2. High-level Architecture

```
                    ┌──────────────────────┐
   Client ──REST──▶ │                      │
   (curl,           │   Orchestrator       │
    Postman,        │   (ASP.NET Core)     │◀──gRPC bidi stream──┐
    load gen)       │                      │                     │
                    │  ┌────────────────┐  │                     │
                    │  │ Scheduler      │  │              ┌──────┴──────┐
                    │  │  + Queue       │  │              │  Worker N   │
                    │  │  + Assignments │  │              │  (C++17)    │
                    │  └────────────────┘  │              │             │
                    │  ┌────────────────┐  │              │ ┌─────────┐ │
                    │  │ EF Core / Npgsql│ │              │ │Executor │ │
                    │  └───────┬─────────┘ │              │ │ pool    │ │
                    └──────────┼───────────┘              │ └─────────┘ │
                               │                          │      │       │
                               ▼                          │      ▼       │
                     ┌──────────────────┐                 │  ┌───────┐   │
                     │   PostgreSQL     │                 │  │ OTel  │   │
                     │  (task ledger,   │                 │  │  SDK  │   │
                     │   results, node  │                 │  └───┬───┘   │
                     │   health)        │                 └──────┼───────┘
                     └──────────────────┘                        │ OTLP/gRPC
                                                                 ▼
                                                        ┌──────────────────┐
                                                        │  OTel Collector  │
                                                        └────────┬─────────┘
                                                                 │
                                                        ┌────────▼─────────┐
                                                        │   Prometheus     │
                                                        └────────┬─────────┘
                                                                 │
                                                        ┌────────▼─────────┐
                                                        │    Grafana       │
                                                        └──────────────────┘
```

Solid arrows = data plane. The orchestrator is the only writer to Postgres. Workers never touch the DB.

---

## 3. Component Responsibilities

### 3.1 Orchestrator (`src/Orchestrator/`, C# / .NET 8)

| Concern | Owner |
|---|---|
| Public REST API (submit, query, cancel) | `JobsController` |
| gRPC service exposed to workers | `WorkerHub` (implements `TaskDispatch.TaskDispatchService`) |
| Task queue + scheduling decisions | `SchedulerService` (`IHostedService`) |
| DB access | `TaskRepository` via EF Core + Npgsql |
| Worker registry & heartbeat tracking | `WorkerRegistry` (in-memory, backed by DB rows) |
| Lease timeout sweeper | `LeaseReaperService` (`IHostedService`) |
| Metrics export (self) | `OpenTelemetry.Extensions.Hosting` |

Design rule: **the scheduler is the only thing that mutates a task's `assigned_worker_id`**. Controllers may enqueue and cancel but never assign.

### 3.2 Worker Node (`src/worker/`, C++17)

| Concern | Owner |
|---|---|
| gRPC stream lifecycle to orchestrator | `OrchestratorClient` |
| Task dispatch to executor threads | `ExecutorPool` (fixed-size `std::thread` pool with a bounded MPMC queue) |
| Per-task resource sampling | `TaskSampler` (samples `getrusage(RUSAGE_THREAD)` + `/proc/self/statm` at 100 ms intervals) |
| Job type registry | `JobRegistry` (compile-time map from `job_type` string → factory) |
| Telemetry export | OpenTelemetry C++ SDK, OTLP/gRPC to collector |
| Graceful shutdown on SIGTERM | `ShutdownCoordinator` (drains in-flight tasks up to `--drain-timeout`) |

Design rule: **a worker never retries a task on its own**. Failures always surface to the orchestrator, which owns retry policy.

### 3.3 Database (PostgreSQL 16)

Sole system-of-record for task state. See §6 for schema. All orchestrator writes go through a single `TaskRepository` so state-transition rules are enforced in one place.

### 3.4 Telemetry stack

- **OpenTelemetry Collector** (contrib image) — receives OTLP from both orchestrator and workers, exports to Prometheus (metrics) and stdout (logs, for now).
- **Prometheus** — scrapes the collector, retains 15 days.
- **Grafana** — pre-provisioned dashboards checked into `deploy/grafana/dashboards/`.

---

## 4. Task Lifecycle (State Machine)

```
                       ┌──────────────┐
     POST /jobs ──────▶│   Pending    │
                       └──────┬───────┘
                              │ scheduler assigns → worker
                              ▼
                       ┌──────────────┐
                       │  Assigned    │◀──── lease renewed
                       └──────┬───────┘
                              │ worker sends TaskStarted
                              ▼
                       ┌──────────────┐
                 ┌────▶│  Processing  │───── worker sends TaskFailed
                 │     └──┬────────┬──┘                │
                 │        │        │                   │
                 │        │        │ TaskCompleted     │
                 │        │        ▼                   ▼
                 │        │   ┌─────────┐        ┌──────────┐
    lease        │        │   │ Success │        │  Failed  │
    expired      │        │   └─────────┘        └────┬─────┘
    (reaper)     │        │                           │ retry policy
                 │        │                           │ (attempts < N)
                 │        ▼                           ▼
                 │   ┌──────────┐                ┌──────────┐
                 └───│  Pending │◀───────────────│  Pending │
                     └──────────┘                └──────────┘
                                                      │
                     POST /jobs/{id}/cancel           │ attempts ≥ N
                              │                       ▼
                              ▼                 ┌──────────────┐
                        ┌───────────┐           │ DeadLettered │
                        │ Cancelled │           └──────────────┘
                        └───────────┘
```

**Invariants** (enforced in `TaskRepository`):

1. Only `Pending → Assigned → Processing → {Success, Failed}` is valid forward flow.
2. `Cancelled` is reachable from `Pending`, `Assigned`, `Processing`.
3. `attempts` is incremented atomically with the transition into `Pending` on retry.
4. Terminal states (`Success`, `DeadLettered`, `Cancelled`) are immutable.

**Lease model.** When the scheduler assigns a task, it writes `lease_expires_at = now() + lease_duration` (default 30 s, or `2 × estimated_runtime` if the job type has one). Workers renew the lease every 10 s while the task is `Processing`. The `LeaseReaperService` scans every 5 s for expired leases and moves them back to `Pending` (with `attempts++`).

---

## 5. API Contracts

### 5.1 Client-facing REST

Base path: `/api/v1`

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a batch of jobs (returns batch id + per-job ids) |
| `GET`  | `/jobs/{id}` | Get a single job's status + result |
| `GET`  | `/jobs?batchId=&status=&limit=` | Query jobs |
| `POST` | `/jobs/{id}/cancel` | Request cancellation |
| `GET`  | `/workers` | List known workers + health |
| `GET`  | `/health/live` | Liveness (always 200 if process is up) |
| `GET`  | `/health/ready` | Readiness (DB reachable, migrations current) |

**Submit request example:**

```json
POST /api/v1/jobs
{
  "jobs": [
    {
      "jobType": "graph.pagerank",
      "priority": 5,
      "payload": {
        "graphRef": "s3://…/graph1.bin",
        "iterations": 20,
        "damping": 0.85
      },
      "maxAttempts": 3,
      "estimatedRuntimeMs": 5000
    }
  ]
}
```

`payload` is opaque JSON to the orchestrator — schema is owned by the worker's job-type registry. Orchestrator only validates size (≤ 64 KiB) and that `jobType` is a known-registered type.

### 5.2 Orchestrator ↔ Worker (gRPC)

Single service, single bidirectional stream. Full `.proto` sketch:

```proto
syntax = "proto3";
package dte.v1;

service TaskDispatch {
  // Long-lived bidirectional stream. Worker opens; orchestrator sends
  // Assignments; worker sends Progress/Result/Heartbeat + Telemetry.
  rpc Stream(stream WorkerMessage) returns (stream OrchestratorMessage);
}

message WorkerMessage {
  oneof kind {
    Hello           hello         = 1;  // sent once at stream start
    Heartbeat       heartbeat     = 2;  // every 5s
    TaskStarted     started       = 3;
    TaskProgress    progress      = 4;  // optional, coarse %
    TaskCompleted   completed     = 5;
    TaskFailed      failed        = 6;
    LeaseRenewal    lease_renew   = 7;
  }
}

message OrchestratorMessage {
  oneof kind {
    Welcome         welcome       = 1;  // sent after Hello
    Assignment      assignment    = 2;
    CancelRequest   cancel        = 3;
    Shutdown        shutdown      = 4;  // graceful drain request
  }
}

message Hello {
  string worker_id      = 1;   // stable UUID persisted at /var/lib/worker/id
  string version        = 2;
  int32  max_parallel   = 3;   // executor pool size
  repeated string job_types = 4; // types this worker can execute
  SystemInfo system     = 5;
}

message Assignment {
  string task_id        = 1;
  string job_type       = 2;
  bytes  payload        = 3;   // JSON bytes, opaque
  int32  attempt        = 4;
  int64  lease_duration_ms = 5;
}

message TaskCompleted {
  string task_id        = 1;
  bytes  result         = 2;   // JSON bytes
  ExecutionMetrics metrics = 3;
}

message ExecutionMetrics {
  int64 wall_ms         = 1;
  int64 cpu_user_ms     = 2;
  int64 cpu_sys_ms      = 3;
  int64 peak_rss_bytes  = 4;
  int64 allocations     = 5;   // opt-in, via a custom allocator wrapper
}
```

Rationale for a single stream: (a) natural backpressure via gRPC flow control, (b) one connection per worker keeps the orchestrator's fd count low, (c) worker disconnect is trivially detected as stream end.

### 5.3 Telemetry channel

**Separate from the control plane.** Workers push metrics via OTLP/gRPC directly to the collector (not through the orchestrator). This prevents metric bursts from starving control messages, and lets the collector own aggregation.

---

## 6. Database Schema (PostgreSQL)

```sql
CREATE TABLE workers (
  id                 UUID PRIMARY KEY,
  version            TEXT NOT NULL,
  max_parallel       INT  NOT NULL,
  registered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  status             TEXT NOT NULL CHECK (status IN ('Online','Draining','Offline'))
);
CREATE INDEX ix_workers_heartbeat ON workers(last_heartbeat_at);

CREATE TABLE job_batches (
  id           UUID PRIMARY KEY,
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  submitter    TEXT
);

CREATE TABLE tasks (
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

-- Hot indexes
CREATE INDEX ix_tasks_pending_priority
  ON tasks(priority DESC, created_at ASC)
  WHERE status = 'Pending';

CREATE INDEX ix_tasks_lease
  ON tasks(lease_expires_at)
  WHERE status IN ('Assigned','Processing');

CREATE INDEX ix_tasks_batch ON tasks(batch_id);

-- Append-only ledger of state transitions (audit + replay)
CREATE TABLE task_events (
  id           BIGSERIAL PRIMARY KEY,
  task_id      UUID NOT NULL REFERENCES tasks(id),
  from_status  TEXT,
  to_status    TEXT NOT NULL,
  worker_id    UUID,
  at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  detail       JSONB
);
CREATE INDEX ix_task_events_task ON task_events(task_id, at);
```

**Why JSONB for payload/result:** avoids a rigid per-job-type schema at the SQL layer while still allowing indexed queries on individual keys if we need them later (e.g. `WHERE payload->>'graphRef' = ...`).

**Why a separate `task_events` table:** the `tasks` row is the current state; `task_events` is history. Cheaper than an audit trigger, and it's the source we hand to Grafana Loki later if we add log-based tracing.

---

## 7. Scheduling & Load Balancing

### 7.1 Algorithm (v1: dead simple)

Every 500 ms, `SchedulerService` runs:

```
tx = db.begin()
pending = SELECT ... FROM tasks
          WHERE status = 'Pending'
          ORDER BY priority DESC, created_at ASC
          LIMIT (Σ worker.available_slots)
          FOR UPDATE SKIP LOCKED;

for task in pending:
    worker = pick_worker_for(task)   // see 7.2
    if worker is None: break
    UPDATE tasks SET status='Assigned',
                     assigned_worker_id=worker.id,
                     lease_expires_at=now() + lease_duration(task)
      WHERE id = task.id;
    INSERT INTO task_events (...);
    send Assignment on worker's stream
tx.commit()
```

`FOR UPDATE SKIP LOCKED` guarantees a second orchestrator instance (future-proofing) would never grab the same task.

### 7.2 Worker selection

Pluggable via `IWorkerSelector`. v1 ships two:

- `RoundRobinSelector` — trivial, fair when tasks are homogeneous.
- `LeastLoadedSelector` — picks the worker with the largest `available_slots / max_parallel`. Default.

Selection must respect `Hello.job_types` — a worker only receives types it advertised.

### 7.3 Backpressure

Workers advertise `max_parallel` and update `available_slots` in every `Heartbeat`. Orchestrator refuses to assign beyond available slots. If total pending > total capacity, tasks stay `Pending` — REST clients see a growing queue depth in `/workers` and Grafana.

---

## 8. Failure Model & Recovery

| Failure | Detection | Recovery |
|---|---|---|
| Worker process crash | gRPC stream closes | All `Assigned`/`Processing` tasks for that worker have leases; reaper returns them to `Pending` after expiry |
| Worker network partition | Missed heartbeats > 3× interval | Worker marked `Offline`; same lease-reaper flow |
| Task exceeds lease (slow) | Lease expiry with no renewal | Task returned to `Pending`, `attempts++`. Worker is *not* killed — it may still complete and its result is discarded (idempotent by construction — see 8.1) |
| Job code exception in worker | Executor catches, sends `TaskFailed` | Retry if `attempts < max_attempts`, else `DeadLettered` |
| Orchestrator crash | External (Docker restart) | On startup, orchestrator scans for `Assigned`/`Processing` tasks with expired leases and reaps immediately; workers reconnect their streams |
| DB unavailable | EF Core throws | Orchestrator `/health/ready` returns 503; scheduler pauses; assignments halted; workers keep heartbeating |
| OTel collector down | Metric export failures | Non-fatal; both orchestrator and workers keep running, drop metrics silently after retry buffer fills |

### 8.1 Duplicate-completion handling

Because a worker may complete a task whose lease has expired and been reassigned, the orchestrator MUST accept only the first terminal transition:

```
UPDATE tasks
SET status='Success', result=$1, finished_at=now()
WHERE id=$2 AND status='Processing' AND assigned_worker_id=$3;
```

If the row-count is 0, the completion is discarded (with a log line + metric `dte_duplicate_completion_total`).

---

## 9. Telemetry Pipeline

### 9.1 Metrics (Prometheus, via OTel Collector)

Orchestrator emits:

- `dte_jobs_submitted_total{batch_source}` (counter)
- `dte_tasks_pending` (gauge)
- `dte_tasks_processing{worker_id}` (gauge)
- `dte_tasks_terminal_total{status,job_type}` (counter)
- `dte_scheduler_loop_seconds` (histogram)
- `dte_lease_reaped_total` (counter)
- `dte_duplicate_completion_total` (counter)

Workers emit (labels: `worker_id`, `job_type`):

- `dte_worker_task_wall_ms` (histogram)
- `dte_worker_task_cpu_ms` (histogram, `mode=user|sys`)
- `dte_worker_task_rss_bytes` (histogram, peak per task)
- `dte_worker_queue_depth` (gauge)
- `dte_worker_available_slots` (gauge)

Cardinality guardrail: `job_type` is a bounded enum (currently 2–5 values), `worker_id` is bounded by cluster size (≤ 200 in the target). Total series: ~50 × 10 buckets each ≈ 500. Safe.

### 9.2 Traces (optional, phase 2)

`POST /jobs` opens a root span; scheduler adds `assign` span; worker adds `execute` span linked via `traceparent` propagated in the `Assignment` message. Emitted OTLP/gRPC to the same collector, exported to Grafana Tempo if we add it later.

### 9.3 Dashboards (checked in as JSON)

- **Cluster Overview**: pending / processing / completed rates, worker count, error rate.
- **Per-Worker**: CPU/RSS heatmaps, task completion latency, queue depth.
- **Per-Job-Type**: p50/p95/p99 wall time, failure rate, retry rate.
- **Health**: scheduler loop time, lease reaper activity, DB latency.

---

## 10. Worker Internals (C++ Concurrency)

### 10.1 Threading model

```
main thread
  ├─ gRPC I/O thread (owned by grpc::CompletionQueue)
  ├─ ExecutorPool: N worker threads (default: hardware_concurrency())
  │    each blocks on ConcurrentBoundedQueue<TaskEnvelope>
  ├─ TelemetryExporter thread (OTel batch export)
  └─ HeartbeatTimer thread (5s tick)
```

- **No shared mutable state between executor threads.** Each `TaskEnvelope` owns its input buffer, output buffer, and per-task allocator arena.
- **Cancellation** is cooperative: the executor checks an `std::atomic<bool>& cancel_flag` at natural checkpoints (loop headers, recursion boundaries). We do *not* use `pthread_cancel`.
- **Per-task allocator** wraps a `std::pmr::monotonic_buffer_resource` so we can measure allocations cheaply and free them all when the task ends (fast reset — no per-object destruction overhead for POD-heavy graph work).

### 10.2 Resource sampling

`TaskSampler` runs on the executor thread itself (no extra thread per task). It records `getrusage(RUSAGE_THREAD)` at start and end, and reads `/proc/self/statm` for RSS. Peak RSS is a coarse process-wide sample; per-task RSS deltas are estimated by watching the allocator arena. Documented as "approximate" in the metric help text.

### 10.3 Job type registry

Compile-time:

```cpp
// worker/jobs/registry.hpp
REGISTER_JOB("graph.pagerank",     GraphPageRankJob);
REGISTER_JOB("graph.bfs",          GraphBfsJob);
REGISTER_JOB("string.suffix_array", SuffixArrayJob);
```

Each job implements:

```cpp
class IJob {
public:
  virtual ~IJob() = default;
  virtual JobResult Run(const JsonView& payload,
                        JobContext& ctx) = 0;
};
```

`JobContext` provides the cancel flag, an arena allocator, and a `LogSpan` for structured logs.

---

## 11. Security Considerations (Local Deployment)

- **Trust boundary:** orchestrator REST is exposed on `localhost:8080` only via compose port binding `127.0.0.1:8080:8080`. No auth.
- **gRPC between orchestrator and workers:** plaintext on the compose network. Fine for a demo; note in the README that production would require mTLS.
- **Payload size limit:** 64 KiB per task, 4 MiB per batch. Enforced by ASP.NET's `MaxRequestBodySize`.
- **Job payload deserialization** on the worker uses `nlohmann::json` in `parse` mode with `allow_exceptions=false`. Any parse error → `TaskFailed`.
- **DB credentials:** injected via env, not baked into images. `POSTGRES_PASSWORD` sourced from a `.env` file that is git-ignored.
- **No arbitrary code execution.** Job types are a closed compiled-in set; the API cannot introduce new code paths.

---

## 12. Docker Compose Topology

```yaml
# docker-compose.yml (sketch)
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: dte
      POSTGRES_USER: dte
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes: [ pgdata:/var/lib/postgresql/data ]
    healthcheck: { test: [ "CMD", "pg_isready", "-U", "dte" ], interval: 5s }

  orchestrator:
    build: ./src/Orchestrator
    depends_on: { postgres: { condition: service_healthy } }
    environment:
      ConnectionStrings__Default: "Host=postgres;Database=dte;Username=dte;Password=${POSTGRES_PASSWORD}"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4317"
    ports: [ "127.0.0.1:8080:8080" ]

  worker:
    build: ./src/worker
    depends_on: [ orchestrator, otel-collector ]
    environment:
      ORCHESTRATOR_ADDR: "orchestrator:5001"
      OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4317"
    deploy:
      replicas: 3           # scale with: docker compose up --scale worker=10

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: [ "--config=/etc/otel/config.yaml" ]
    volumes: [ ./deploy/otel:/etc/otel:ro ]

  prometheus:
    image: prom/prometheus:latest
    volumes: [ ./deploy/prometheus:/etc/prometheus:ro ]

  grafana:
    image: grafana/grafana:latest
    ports: [ "127.0.0.1:3000:3000" ]
    volumes:
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/grafana/dashboards:/var/lib/grafana/dashboards:ro

volumes:
  pgdata:
```

Scaling workers is a first-class use case: `docker compose up --scale worker=10 -d`.

---

## 13. Phased Build Plan

Each phase is meant to end at a **demoable state**.

### Phase 0 — Repo skeleton (½ day)
- Solution + project layout, `.editorconfig`, `clang-format`, `.gitignore`.
- Compose file with just `postgres` and Grafana (empty dashboard).
- **Demo:** `docker compose up postgres grafana` succeeds.

### Phase 1 — Orchestrator + REST + DB (2 days)
- EF Core model + initial migration.
- `POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`.
- No workers yet — a task submitted just sits at `Pending`.
- xUnit tests for the state-machine invariants in `TaskRepository`.
- **Demo:** curl a batch in, see rows in Postgres, cancel one, see `Cancelled`.

### Phase 2 — gRPC contract + fake worker (1 day)
- `.proto` finalized, code-generated for C# and C++.
- A C# console app impersonating a worker: opens the stream, sleeps, marks tasks done.
- Scheduler v1 (least-loaded selector), lease reaper.
- **Demo:** submit 100 jobs, fake worker drains them, all become `Success`.

### Phase 3 — Real C++ worker, one job type (3 days)
- `ExecutorPool`, `OrchestratorClient`, `JobRegistry`.
- Ship `graph.bfs` as the first real job.
- Dockerfile for the worker (multi-stage: build image with grpc-cpp devtools, runtime image with just the binary).
- **Demo:** compose up with 2 worker replicas, submit 1000 BFS jobs, all succeed.

### Phase 4 — Telemetry end-to-end (2 days)
- OTel Collector + Prometheus + Grafana wired up.
- Worker and orchestrator instrumentation.
- Cluster Overview + Per-Worker dashboards.
- **Demo:** submit a large batch, watch throughput/latency panels update live.

### Phase 5 — Second job type + failure story (2 days)
- Add `string.suffix_array` (a good stress test — CPU-bound, high RSS).
- Explicit "kill a worker mid-batch" demo: `docker kill dte-worker-2`, watch reaper redistribute, everything still completes.
- Add `dte_duplicate_completion_total` panel to prove the invariant.

### Phase 6 — Polish (1–2 days)
- README with architecture diagram + one-command run.
- `bench/` folder with a simple submission loadgen (`k6` or a small C# console app).
- Screenshots of Grafana under load.
- CI: build C++ + C#, run tests, build docker images.

**Total: ~11 focused days.** Realistic calendar time for a student juggling coursework: 3–4 weeks.

---

## 14. Risks & Open Questions

| # | Risk | Mitigation / Decision |
|---|---|---|
| R1 | grpc-cpp is heavy to build on Windows | Do C++ work inside the Docker builder image, not on host. Recommend WSL2 for local dev. |
| R2 | Windows-native C++ worker missing (`getrusage`, `/proc`) | Explicitly Linux-target the worker container. Document that the worker only runs in Docker/Linux. Orchestrator + REST usable natively on Windows for dev. |
| R3 | Postgres bottleneck if scheduler loop is chatty | Batch inserts to `task_events`; keep hot path indexes; measure `dte_scheduler_loop_seconds` from day 1. |
| R4 | Metric cardinality blow-up if `worker_id` grows unbounded | Bounded by compose scale; add a `worker_pool` label instead of `worker_id` in aggregate dashboards. |
| R5 | Bidi stream reconnection edge cases | Worker uses exponential backoff (100 ms → 30 s); on reconnect, re-sends `Hello`; orchestrator wipes stale `Assigned` for that worker on `Hello`. |
| R6 | JSONB payloads make schema drift silent | Version job types (`graph.bfs.v1`); registry only advertises known versions. |
| Q1 | Do we need cancellation propagation into `Processing` tasks? | Yes for v1 — cooperative cancel flag. Documented cost: jobs must check periodically. |
| Q2 | Priority levels — how many? | Start with 0–9 int; only two effective tiers (high=8+, normal=default). |
| Q3 | Result size limit? | 256 KiB per result. Larger results should be side-channeled (blob store) later. |

---

## 15. Success Criteria / Demo Script

The finished project must be able to:

1. **Cold-start** with `docker compose up -d` and reach a "ready" cluster in under 60 s.
2. **Submit 10,000 jobs** across two job types via a single API call and see all reach terminal state.
3. **Survive a kill.** `docker kill` a worker mid-run; every job still terminates (no orphans, no duplicates in `Success`).
4. **Show live metrics.** Grafana dashboards update within 15 s of task completion.
5. **Explain itself.** README diagram matches the running system; the sequence "submit → dispatch → execute → persist → visualize" is walkable in the code in 5 minutes.

If all five hold, the design is validated.

---

## Appendix A — Directory Layout (target)

```
DistributedTaskEngine/
├── DESIGN.md                   ← this doc
├── README.md
├── docker-compose.yml
├── .env.example
├── src/
│   ├── Orchestrator/           C# / .NET 8
│   │   ├── Orchestrator.csproj
│   │   ├── Program.cs
│   │   ├── Api/                Controllers, DTOs
│   │   ├── Grpc/               WorkerHub, generated stubs
│   │   ├── Scheduling/         SchedulerService, IWorkerSelector
│   │   ├── Persistence/        DbContext, TaskRepository, Migrations
│   │   ├── Telemetry/          OTel setup, metric definitions
│   │   └── Domain/             Task, State, Events
│   ├── Contracts/              *.proto files (single source of truth)
│   └── worker/                 C++17
│       ├── CMakeLists.txt
│       ├── Dockerfile
│       ├── src/
│       │   ├── main.cpp
│       │   ├── orchestrator_client.{hpp,cpp}
│       │   ├── executor_pool.{hpp,cpp}
│       │   ├── task_sampler.{hpp,cpp}
│       │   ├── telemetry.{hpp,cpp}
│       │   └── jobs/
│       │       ├── registry.hpp
│       │       ├── graph_bfs.cpp
│       │       └── suffix_array.cpp
│       └── tests/              GoogleTest
├── deploy/
│   ├── otel/config.yaml
│   ├── prometheus/prometheus.yml
│   └── grafana/{provisioning,dashboards}/
├── tests/
│   ├── Orchestrator.Tests/     xUnit + Testcontainers-Postgres
│   └── Integration/            docker-compose based e2e
└── bench/
    └── loadgen/                small submission client
```

## Appendix B — Key External Dependencies

| Layer | Package | Notes |
|---|---|---|
| C# web/gRPC | `Microsoft.AspNetCore.App`, `Grpc.AspNetCore` | .NET 8 LTS |
| C# ORM | `Npgsql.EntityFrameworkCore.PostgreSQL` | v8.x |
| C# telemetry | `OpenTelemetry.Extensions.Hosting`, `.Exporter.OpenTelemetryProtocol` | |
| C# tests | `xUnit`, `Testcontainers.PostgreSql` | real DB in tests, no mocks |
| C++ gRPC | `grpc` (via vcpkg or apt in build image) | |
| C++ telemetry | `opentelemetry-cpp` with OTLP/gRPC exporter | |
| C++ JSON | `nlohmann/json` | header-only, exception-free mode |
| C++ tests | `googletest` | |
| Infra | `otel/opentelemetry-collector-contrib`, `prom/prometheus`, `grafana/grafana`, `postgres:16` | |

---
*End of design document. Reviewers: please leave comments in-line or file issues against §14 (Risks) before Phase 1 kickoff.*
