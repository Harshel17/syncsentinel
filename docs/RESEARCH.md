# Stacksync research

Research was performed on 2026-11-25 against the four requested official pages and the linked List sync issues reference. All statements below are separated into verified facts, design assumptions, and simulator-only behavior.

## Verified facts

### Webhook delivery and ordering

From the official [Sync webhooks documentation](https://docs.stacksync.com/two-way-sync/features/sync-webhooks):

- A webhook is a signal, not one notification per issue. `real_time_issue_notification` is emitted per failed batch and contains no issue ID or record count.
- Delivery is best effort and attempted exactly once. Stacksync does not inspect the receiver response, retry a failed delivery, expose notification history, or provide replay.
- Calls may be concurrent, bursty, repeated, and out of order. `created_time` is explicitly not an ordering guarantee.
- A notification may arrive shortly before its issues are visible through the API.
- Some issues produce no notification, and issue resolution produces no notification. The docs explicitly prescribe scheduled polling in addition to webhook-triggered reads.
- Notifications use `Authorization: Bearer <api_key>`. They are not cryptographically signed.

Therefore, "at most one delivery attempt" is the precise transport guarantee; it must not be described as exactly-once processing.

### Notification payload

Every payload is a JSON object with one `notification` property. Always-present fields are `created_time`, `notification_type`, `msg`, and `base_id`. A `real_time_issue_notification` also contains `app_id` and `internal_table_id`.

The three documented notification types are:

- `real_time_issue_notification`: a failed write; sync continues;
- `base_activity`: Stacksync stopped the sync; the payload does not identify the app or structured error;
- `backfill`: full reads and their pending writes ended, but individual records may still have failed.

The payload has no event ID, workspace ID, region, issue ID, or affected-record count. `msg` is human-facing and must not be parsed.

### Issue discovery

The [List sync issues API](https://docs.stacksync.com/api-reference/syncs/issues/list_sync_issues) is:

```text
GET /v1/workspaces/{workspace_id}/issues?region={besg|usnv}
```

It supports filters including `base_id`, `app_id`, `table_id`, and `issue_id`, plus page number/size and ordering. Results are paginated and ordered newest-first by `last_modified_time` by default. Documented issue fields include `issue_id`, sync and destination coordinates, attempted action, message, changed columns, source/destination record IDs, and timestamps.

The official OpenAPI schema clarifies that `destination_table_id`, `destination_table_name`, and `changed_columns` are optional, and `changed_columns` may be null. Pagination requires `current_page`; `next_page` is present only when another page exists, and `total_pages` is absent for an empty page. SyncSentinel follows `next_page` rather than inferring continuation from `total_pages` or page length.

For an open record issue, a repeated failure overwrites the issue and assigns a new `issue_id`, while the stable record coordinates remain usable for correlation. The webhook guide recommends deduplicating failure occurrences by `issue_id` and following a record with base/app/table/record identity.

### Issue resolution and retry behavior

The [Resolve sync issues API](https://docs.stacksync.com/api-reference/syncs/issues/resolve_sync_issues) is:

```text
POST /v1/workspaces/{workspace_id}/issues/action?region={besg|usnv}
```

For explicitly listed `ids`, `action` is one of `retry`, `revert`, or `ignore`. The documented response reports IDs, success/failure counts, and an error list. The page also describes an `all_issues` variant; this foundation intentionally models only explicit IDs to keep recovery scope bounded.

The official [error handling and replay article](https://www.stacksync.com/blog/two-way-sync-error-handling-retries-replay) says:

- Retry reads the source record's latest values and attempts the write again. It is current-state repair, not immutable historical replay.
- Revert reads destination values and writes them to the source.
- Ignore removes the issue and can leave the systems divergent.
- Stacksync supports individual and bulk resolution, but successful API execution is not proof of the intended business outcome. The article recommends checking both systems and downstream effects.

### REST API retry guidance

The official [API errors reference](https://docs.stacksync.com/api-reference/errors) documents current error bodies as `{error, msg, field}`, while older sync issue operations return at least `{error}`. It recommends retrying only:

- HTTP 400 with `error: "Unexpected Error"`; or
- HTTP 409 when the same `Idempotency-Key` request is still running.

Other 4xx responses require correcting the request, key, permissions, state, or region. No API rate limit is published. Most responses include `X-Request-Id`.

## Relevance and overlap

Stacksync already provides issue detection, an Issues dashboard, and retry/revert/ignore actions. SyncSentinel must not present those as inventions.

The demonstration remains relevant because the public docs explicitly expose a reliability integration problem: lossy/unordered notifications require durable ingestion plus polling; recovery actions need policy control; a successful action requires independent verification; and external execution history is useful for crash recovery and audit. SyncSentinel composes the existing APIs into that control loop. It would be duplicative if reduced to another Issues dashboard or a button that merely calls `retry`.

## Design assumptions (not verified product facts)

- A demo workspace and sync registry will be configured locally with `workspace_id`, `base_id`, and `region`, because webhook payloads omit workspace and region.
- Scheduled reconciliation every few minutes is appropriate for a demo; the exact interval must be configurable and tested against real account behavior.
- Verification adapters can read representative source and destination state. The public issue API alone cannot prove end-to-end convergence.
- Recovery policy will begin deny-by-default and allow automatic action only for explicit, tested classifications. Free-text `issue_message` will not be treated as a stable error code.
- SQLite on one process is sufficient for this four-hour demonstration, not for horizontally scaled production deployment.

## Simulator-specific behavior

- The simulator implements only the two verified issue routes above and uses documented field names.
- Its data store is in memory and seeded by the test harness.
- Any requested action removes a known issue immediately; this is a deterministic test convenience, not a statement about Stacksync timing or internals.
- Missing IDs are represented in `error_list` for useful failure testing. The precise per-ID error entry shape is not visible in the requested rendered documentation and must not be treated as verified.
- Phase 2 adds explicit `/simulator/.../source/...` and `/destination/...` record reads. These are invented simulator APIs for independent verification, not public Stacksync endpoints.
- Simulator retry can copy current source fields to the destination deterministically. This models current-state repair for tests, not real connector timing or side effects.
- Both simulated record sides are addressed with destination app/table coordinates. This field mapping is a local fixture convention, not a verified universal mapping.
- The simulator still does not model real authentication, visibility races, rate behavior, or universal connector semantics.

## Documentation access

All requested documentation was accessible. The public OpenAPI document was also accessible, but no live Stacksync account calls were made.
