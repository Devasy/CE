# Practical Architecture Template

This template is intentionally split into 3 parts for easier reuse.

## 1) FastAPI Specific
### Goal
Create a clean API layer with predictable request handling and modular growth.

### Suggested Structure
```text
app/
  api/
    main.py
    routers/
    middleware/
    dependencies/
  services/
  models/
  schemas/
```

### What to Do
- Keep route handlers thin and move business logic to services.
- Use typed request and response schemas.
- Add one global exception boundary for unknown runtime failures.
- Keep middleware focused: auth, request-id, feature toggle, and logging.
- Use dependency injection for auth/session/config access.
- Keep feature toggles enforced in backend logic, not only UI.

### What to Avoid
- Avoid putting DB logic directly inside route handlers.
- Avoid broad exception swallowing without structured logging.
- Avoid API contracts that return mixed response shapes for the same endpoint.

## 2) MongoDB Specifics (and FastAPI Integration)
### Goal
Make data access explicit, safe, and scalable for API workloads.

### Data Access Pattern
- Create a repository/data-access layer per domain.
- Create and validate indexes at startup or in migrations.
- Use cursor-based pagination for large datasets.
- Use projection to avoid over-fetching large documents.

### FastAPI + MongoDB Runtime Pattern
- Initialize Mongo client once at startup and reuse it.
- Keep collection names/constants centralized.
- Validate env-based DB settings at startup before serving requests.
- Use request-scoped sessions where transactions are required.

### Migrations Pattern
- Keep one script per version.
- Keep one ordered ALL_VERSIONS list with newest at top.
- Keep initialize.py only for first-time setup.
- Keep migrate.py as the only orchestrator.
- Move databaseVersion forward only after each successful script.
- Append migration history only when full upgrade succeeds.

### What to Avoid
- Never use skip/limit pagination for very large collections.
- Never execute side-effectful migration code at module import time.
- Never drop indexes without existence checks.
- Never run critical multi-collection rewrites without transactions when replica set support exists.
- Never use shell=True for migration subprocess orchestration.

## 3) Celery with FastAPI and MongoDB
### Goal
Run async workloads safely with clear scheduling, locking, and observability.

### Core Pattern
- Use one central task-priority map for routing.
- Use quorum queues for HA behavior.
- Use a Mongo-backed scheduler with beat-lock checks to avoid duplicate scheduling in HA.
- Track task lifecycle in DB (in queue, running, success, failed).
- Route dispatch through one wrapper that injects lock metadata from a lock registry.

### Scheduler and Worker Safeguards
- Cache scheduler settings and beat-lock reads for short TTL windows.
- Guard positional task arguments before index access.
- Guard chained dict reads where intermediate values may be None.
- Use finite retry with backoff for broker failures.
- Clamp numeric env-based settings (for example concurrency cannot go below 1).
- If using shared IPC files for autoscaler coordination, open with creation fallback and proper locking.

### What to Avoid
- Never mutate a shared module-level lock map during scheduling.
- Never derive backend URL from broker URL via string replacement.
- Never default to pickle serializer unless strictly required and isolated.
- Never use private framework config attributes.
- Never do per-entry DB calls inside every scheduler tick without caching.
- Never retry forever without alerting/circuit-breaker behavior.

## Minimum Acceptance Criteria
- API has clear service boundaries and global error handling.
- Mongo data access is repository-based and migration-driven.
- Celery scheduling is lock-safe and queue-priority aware.
- Retry logic is bounded and observable.
- Large data migrations are batched and cursor-based.
