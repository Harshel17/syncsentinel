# Implementation plan

## Completed through Phase 2

- Repository and Python packaging initialized.
- Public webhook, list-issue, and resolve-issue contracts modeled.
- Deterministic domain identity and explicit state transitions implemented.
- SQLite schema, transactional incident ingestion, deduplication, optimistic concurrency, and audit history implemented.
- In-memory FastAPI simulator and foundational tests implemented.
- Research and architecture decisions documented.
- Durable webhook-hint ingestion and burst coalescing implemented.
- Scheduled paginated reconciliation and bounded durable backoff implemented.
- Exclusive reconciliation, recovery, and verification leases implemented.
- Operator-approved retry validates the current issue occurrence before execution.
- Ambiguous outcomes go to verification without blind external retry.
- Simulator-only record reads support independent field-level verification.
- Structured audit history and restart lease recovery implemented.
- Deterministic Phase 2 acceptance scenarios implemented.

## Recommended Phase 3: harden the demonstrated slice

1. Add explicit schema migrations rather than create-only bootstrap SQL.
2. Add lease renewal for work that can exceed the demonstration lease.
3. Add configurable verification retry windows before manual review.
4. Validate the HTTP adapter against a Stacksync sandbox and capture real partial-failure bodies.
5. Add connector-specific verification adapters behind the existing interface.
6. Add operational health/readiness endpoints and audit pagination.
7. Only then build the small React/TypeScript operator view.

## Explicitly deferred

- React/TypeScript UI.
- Real Stacksync credentials or customer data.
- Automatic `revert` or `ignore`.
- Multi-process deployment, distributed locks, external queues, metrics stack, and authentication.
- Historical event replay, because the documented recovery action uses current source values.

## Open validation questions

- What is the exact `error_list` item schema for partially successful issue resolution?
- Does the resolution route honor an `Idempotency-Key`, and if so, for how long? The requested page does not document it.
- How long can issue visibility lag after a notification in realistic conditions?
- What does a stale overwritten `issue_id` return from resolution: 400 or 404, and with what stable error value?
- Which connector read APIs are appropriate for independent source/destination verification in a real demo account?
