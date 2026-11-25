"""Regression tests for defects found in the independent review."""

from __future__ import annotations

from datetime import timedelta

from fastapi.testclient import TestClient

from syncsentinel.contracts import ListIssuesResponse, SyncIssue
from syncsentinel.domain import Incident, IncidentState
from syncsentinel.gateway import AmbiguousOutcome, HttpStacksyncGateway
from syncsentinel.persistence import IncidentRepository, SyncTarget
from syncsentinel.services import ReconciliationEngine, RecoveryEngine
from syncsentinel.simulator import SimulatedStacksync, create_simulated_stacksync


def _repo(tmp_path, sync_issue, now):
    repository = IncidentRepository(tmp_path / "review.sqlite3")
    repository.initialize()
    target = SyncTarget(42, "usnv", sync_issue.base_id, poll_interval_seconds=60)
    repository.register_target(target, now)
    return repository, target


def _discover(repository, target, gateway, now):
    repository.schedule_reconciliation(target, now, "discover", reason="scheduled_poll")
    assert ReconciliationEngine(repository, gateway).run_once("reconciler", now)


def _incident(repository, issue):
    return repository.get(Incident.from_observation(issue.to_observation()).id)


def _keys(issue):
    app, table = issue.destination_app_id, issue.destination_table_id
    return (app, table, issue.source_record_id), (app, table, issue.destination_record_id)


class NeverExecutes(SimulatedStacksync):
    """Request times out before reaching Stacksync: nothing was retried."""

    def retry_issue(self, target, issue_id):
        self.retry_calls.append(issue_id)
        raise AmbiguousOutcome("connect timeout")


def test_empty_changed_columns_must_not_verify_as_converged(tmp_path, issue_payload, fixed_time):
    issue = SyncIssue.model_validate({**issue_payload, "changed_columns": {}})
    src, dst = _keys(issue)
    simulator = NeverExecutes(
        [issue],
        source_records={src: {"website": "new"}},
        destination_records={dst: {"website": "totally-different"}},
    )
    repository, target = _repo(tmp_path, issue, fixed_time)
    _discover(repository, target, simulator, fixed_time)
    incident = _incident(repository, issue)
    repository.approve_retry(incident.id, incident.latest_issue_id, "op", "c1", fixed_time)
    engine = RecoveryEngine(repository, simulator)
    engine.run_recovery_once("w", fixed_time)
    engine.run_verification_once("w", fixed_time)

    # Records differ, retry never ran, issue is still open: must not be resolved.
    assert repository.get(incident.id).state is not IncidentState.RESOLVED


def test_open_stacksync_issue_must_not_be_reported_resolved(tmp_path, sync_issue, fixed_time):
    src, dst = _keys(sync_issue)
    # Field values already happen to match (e.g. someone fixed the destination by hand).
    simulator = NeverExecutes(
        [sync_issue],
        source_records={src: {"website": "same"}},
        destination_records={dst: {"website": "same"}},
    )
    repository, target = _repo(tmp_path, sync_issue, fixed_time)
    _discover(repository, target, simulator, fixed_time)
    incident = _incident(repository, sync_issue)
    repository.approve_retry(incident.id, incident.latest_issue_id, "op", "c2", fixed_time)
    engine = RecoveryEngine(repository, simulator)
    engine.run_recovery_once("w", fixed_time)
    engine.run_verification_once("w", fixed_time)

    # The next scheduled poll still sees the same open issue in Stacksync.
    later = fixed_time + timedelta(minutes=5)
    _discover(repository, target, simulator, later)

    assert sync_issue.issue_id in simulator.issues  # Stacksync: still open
    assert repository.get(incident.id).state is not IncidentState.RESOLVED


def test_webhook_hint_during_running_scan_schedules_follow_up(tmp_path, sync_issue, fixed_time):
    repository, target = _repo(tmp_path, sync_issue, fixed_time)
    repository.schedule_reconciliation(target, fixed_time, "poll", reason="scheduled_poll")
    running = repository.claim_reconciliation("w", fixed_time)
    # Scan has already read its pages; a webhook now announces a new failure.
    _, scheduled = repository.schedule_reconciliation(target, fixed_time, "hint")
    repository.finish_reconciliation(running, fixed_time)

    assert repository.claim_reconciliation("w", fixed_time) is not None, (
        f"hint lost (scheduled={scheduled}); next read only at the scheduled poll"
    )


def test_contract_accepts_documented_empty_page_and_optional_fields(issue_payload):
    # Docs: total_pages is "absent when the page is empty"; destination_table_id,
    # destination_table_name and changed_columns are optional.
    ListIssuesResponse.model_validate(
        {"data": [], "pagination": {"current_page": 0}, "total_number_of_issues": 0}
    )
    minimal = {k: v for k, v in issue_payload.items()
               if k not in {"destination_table_id", "destination_table_name", "changed_columns"}}
    SyncIssue.model_validate(minimal)


def test_missing_destination_record_over_http_goes_to_manual_review(
    tmp_path, sync_issue, fixed_time
):
    src, _ = _keys(sync_issue)
    simulator = SimulatedStacksync(
        [sync_issue], source_records={src: {"website": "x"}}, converge_on_retry=False
    )
    gateway = HttpStacksyncGateway(
        "http://testserver",
        client=TestClient(create_simulated_stacksync(simulator=simulator)),
    )
    repository, target = _repo(tmp_path, sync_issue, fixed_time)
    _discover(repository, target, gateway, fixed_time)
    incident = _incident(repository, sync_issue)
    repository.approve_retry(incident.id, incident.latest_issue_id, "op", "h3", fixed_time)
    engine = RecoveryEngine(repository, gateway)
    engine.run_recovery_once("w", fixed_time)

    assert engine.run_verification_once("w", fixed_time)  # raises HTTPStatusError today
    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW


def test_stale_recovery_worker_cannot_clobber_verification(tmp_path, sync_issue, fixed_time):
    simulator = SimulatedStacksync([sync_issue])
    repository, target = _repo(tmp_path, sync_issue, fixed_time)
    _discover(repository, target, simulator, fixed_time)
    incident = _incident(repository, sync_issue)
    repository.approve_retry(incident.id, incident.latest_issue_id, "op", "h4", fixed_time)

    stale = repository.claim_recovery("worker-a", fixed_time, lease_seconds=30)
    later = fixed_time + timedelta(seconds=31)
    repository.recover_expired_leases(later, "restart")
    verifier = repository.claim_verification("worker-b", later)

    # worker-a wakes up after a slow validation read and rejects its (lost) job.
    try:
        repository.reject_stale_recovery(stale, later, "validation timed out")
    except RuntimeError:
        pass
    repository.finish_verification(verifier, later, converged=False, reason="mismatch")
    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW
