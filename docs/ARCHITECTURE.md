# Architecture

## Scope

SyncSentinel is a local FastAPI application with SQLite and deterministic workers, accompanied by a focused React/TypeScript operator console. The system wraps Stacksync's documented issue operations; it never pretends to be the sync engine.

## Intended control loop

```text
Stacksync webhook ─┐
                   ├─> durable work signal ─> issue poller ─> incident upsert
periodic schedule ─┘                                      │
                                                         v
operator/policy ─> approved recovery ─> Stacksync action ─> independent verifier
                           │                    │                    │
                           └──────── immutable execution history ───┘
```

The webhook path should authenticate and commit only a small work signal before replying. A scheduled scan is the correctness path; webhooks reduce detection latency. Coalescing by base/table controls bursts, but issue occurrence deduplication uses `issue_id`.

## Domain identity

Stacksync documents that `issue_id` changes when the same record fails again. SyncSentinel therefore uses two identities:

- occurrence identity: `issue_id`, unique in `issue_observations` for idempotent ingestion;
- incident identity: SHA-256 of `base_id`, destination app, destination table, and source record ID (falling back to destination record ID), then a deterministic UUIDv5.

This preserves one incident timeline across recurrence without confusing two legitimate records. If both record IDs are absent, the foundation conservatively hashes the occurrence, avoiding accidental merging.

## State machine

```text
DISCOVERED -> AWAITING_APPROVAL -> APPROVED -> EXECUTING -> VERIFYING -> RESOLVED
    │                 │              │            │            │
    └─────────────────┴──────────────┴────────────┴────> MANUAL_REVIEW
                                                               │
                                                               └-> AWAITING_APPROVAL / RESOLVED
RESOLVED --new failure occurrence--> DISCOVERED
```

There is deliberately no direct `EXECUTING -> RESOLVED` transition. An API success only authorizes verification. Invalid transitions fail closed.

## Transactional model

SQLite tables are:

- `incidents`: current durable state and optimistic `version`;
- `issue_observations`: immutable, unique Stacksync failure occurrences;
- `incident_events`: append-only audit history.

Observation insertion, incident upsert, and audit append occur inside one `BEGIN IMMEDIATE` transaction. State changes use an expected version and append their audit event in the same transaction.

Reconciliation and recovery jobs use expiring durable leases. SQLite write locking prevents two workers from claiming one job. An expired reconciliation lease becomes pending again. An expired recovery execution lease becomes an ambiguous outcome and moves to verification; the external action is never blindly repeated. Reconciliation reads use bounded exponential backoff with deterministic `available_at` timestamps.

## Policy-gated recovery

The foundation policy is deny-by-default. Planned policy inputs are structured attributes and explicit operator approval—not parsing mutable human error messages. `revert` and `ignore` should require manual approval initially because each can intentionally overwrite or preserve divergence. Even `retry` is current-state repair and should be automatic only for a narrow tested allowlist.

## Independent verification

Resolution requires a verifier different from the recovery API response. Phase 2 reads the simulated source, reads the simulated destination, reads the source again to detect a concurrent change, compares a non-empty set of fields named by `changed_columns`, and confirms through the issue-list API that the approved occurrence is no longer open. Missing fields or records route the incident to manual review.

Record reads, issue closure, and the final SQLite transition are separate observations. They are not an atomic cross-system snapshot: a source record or remote issue can change between reads. Subsequent polling reopens a resolved incident if the same issue is observed open again.

These record-read routes are simulator-specific. The researched documentation does not establish a universal Stacksync endpoint for reading both connector records. Downstream business side effects remain outside what this demonstration can prove.

The simulator maps both sides using the issue's destination app and table identifiers plus separate source/destination record IDs. This is a deterministic demo convention, not a documented universal Stacksync connector mapping.

## Lease fencing and reruns

Every work claim receives a random lease token in addition to the human-readable worker ID. Mutations require the expected status and token, preventing a stale callback from a reused worker identity from changing newer work. A webhook arriving during a leased reconciliation sets `rerun_requested` in the same durable row; completion atomically converts that marker into a pending follow-up scan.

## Audit model

`audit_events` stores an event type, UTC timestamp, correlation ID, optional incident and job IDs, optional state transition, and structured JSON details. It covers webhook receipt, scheduling and coalescing, issue discovery and duplicate suppression, recovery approval and execution, ambiguous outcomes, verification, worker restart, and lease recovery.

## Trade-offs

- SQLite gives atomicity, restart durability, and inspectable state with minimal setup; it intentionally limits horizontal concurrency.
- A deterministic state machine is more explicit than a general workflow framework and fits the four-hour constraint.
- Polling is required for correctness because the documented webhook is lossy; it adds API traffic and detection latency.
- Stable-record correlation improves recurrence handling but is not event replay. Intermediate historical values are unavailable.
- The simulator mirrors verified routes while deliberately omitting unverified connector behavior.

## Demonstration boundary

The console's scenario, reset, and worker-cycle endpoints are local demonstration controls, not Stacksync capabilities. They are intentionally unauthenticated and must only be bound to loopback. Source/destination reads and their app/table/record mapping are likewise simulator contracts.

Issue closure, record reads, and local state transitions cannot be observed atomically across systems. A passing verification is evidence from bounded sequential observations, not proof of a globally consistent snapshot. Unknown external outcomes therefore go to verification, empty or missing evidence goes to manual review, and later reconciliation is allowed to reopen a resolved incident.
