# Master Prompt (3-Part Structure)

Use this as a simple, reusable prompt for any agent building FastAPI, MongoDB, and Celery together.

## Copy-Paste Prompt
```text
You are an expert backend implementation agent.

Build a production-ready backend in exactly 3 parts:

PART 1: FASTAPI SPECIFIC
- Create clean app structure: api, routers, middleware, services, schemas.
- Keep handlers thin and push business logic to services.
- Add global exception handling and structured logging.
- Enforce feature toggles and auth in backend middleware/dependencies.

PART 2: MONGODB SPECIFICS (WITH FASTAPI)
- Use one startup-initialized Mongo client and reuse it safely.
- Implement repository/data-access layer per domain.
- Add index management and schema validation strategy.
- Implement migrations with initialize.py + migrate.py + one script per version.
- Keep an ordered ALL_VERSIONS list (newest first) and validate script-file existence.
- Use cursor-based pagination for large collections.

PART 3: CELERY WITH FASTAPI AND MONGODB
- Use centralized TASK_PRIORITIES for queue routing.
- Use quorum queues.
- Use Mongo-backed scheduler with beat-lock checks for HA scheduling.
- Use lock metadata registry and inject lock args through one dispatch wrapper.
- Track task lifecycle in DB (queued, running, success, failed).
- Cache scheduler settings/lock reads for short TTL to reduce DB load.

Hard constraints (must follow):
- No mutable default arguments.
- No shell=True in migration orchestration.
- No broad exception swallowing without structured logs.
- Do not mutate shared module-level lock maps at runtime.
- Do not derive backend URL from broker URL by string replacement.
- Do not use private framework config attributes.
- Do not use infinite retry loops; use finite retry with backoff.
- Guard env-based numeric values and clamp minimum safe values.
- Guard positional args and nested dict access in scheduler code.

Deliverables:
1) Short architecture doc in 3 sections (FastAPI, MongoDB, Celery).
2) Minimal runnable code skeleton.
3) Migration skeleton and one sample version migration.
4) Test plan covering API, scheduler lock behavior, retries, and migration path.

Definition of done:
- API starts and health endpoint works.
- Mongo repositories and migration runner are wired.
- Celery worker and scheduler run with queue priority routing.
- Scheduler lock behavior prevents duplicate task firing in HA.
- Tests cover critical flow and failure scenarios.
```

## Quick Variants
### Docs Only
```text
Skip implementation. Produce architecture, diagrams, and rollout plan only.
```

### Implementation First
```text
Prioritize runnable code and tests. Keep docs concise.
```

## Reviewer Checklist
- 3-part structure is followed clearly.
- API layer keeps business logic out of routers.
- Mongo access is repository-based and migration-driven.
- Celery scheduler uses lock-safe, cache-aware behavior.
- Retry logic is finite and observable.
