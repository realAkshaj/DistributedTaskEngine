# Distributed Task Execution & Telemetry Engine

A local-first distributed execution engine. C# / ASP.NET Core orchestrator dispatches compute jobs over gRPC to a pool of C++ worker nodes. Task state lives in PostgreSQL. Workers stream OpenTelemetry metrics through the OTel Collector → Prometheus → Grafana.

Full architecture and phased build plan: **[DESIGN.md](./DESIGN.md)**.

## Status

Design phase. No code yet.

## Stack

| Layer | Tech |
|---|---|
| Orchestrator | C# / ASP.NET Core (.NET 8) |
| Workers | C++17 |
| Transport | gRPC bidirectional streaming |
| Client API | REST |
| Database | PostgreSQL 16 |
| Telemetry | OpenTelemetry → Prometheus → Grafana |
| Runtime | Docker Compose |

## Run (once implemented)

```bash
cp .env.example .env
docker compose up -d
docker compose up --scale worker=5 -d
```

- Orchestrator API: `http://localhost:8080`
- Grafana: `http://localhost:3000`

## Repo layout

See Appendix A in [DESIGN.md](./DESIGN.md).
