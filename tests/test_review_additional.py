from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from syncsentinel.app import create_app
from syncsentinel.contracts import (
    ResolveIssuesResponse,
    ResolvedSyncIssues,
    SyncIssue,
)
from syncsentinel.domain import Incident, IncidentState
from syncsentinel.gateway import HttpStacksyncGateway, TransientAPIError
from syncsentinel.persistence import (
    IncidentRepository,
    LeaseConflict,
    RecoveryApprovalConflict,
    StaleOccurrence,
    SyncTarget,
)
from syncsentinel.services import ReconciliationEngine, RecoveryEngine
from syncsentinel.simulator import SimulatedStacksync, create_simulated_stacksync
from syncsentinel.worker import run_tick


def setup_repository(tmp_path, issue, now):
    repository = IncidentRepository(tmp_path / "additional.sqlite3")
    repository.initialize()
    target = SyncTarget(42, "usnv", issue.base_id, 60)
    repository.register_target(target, now)
    return repository, target


def discover(repository, target, gateway, now):
    repository.schedule_reconciliation(target, now, "discover", reason="scheduled_poll")
    assert ReconciliationEngine(repository, gateway).run_once("worker", now)
    return repository.get(Incident.from_observation(
        next(iter(gateway.issues.values())).to_observation()
    ).id)


def stores(issue, value="same"):
    return (
        {(issue.destination_app_id, issue.destination_table_id, issue.source_record_id): {
            "website": value
        }},
        {(issue.destination_app_id, issue.destination_table_id, issue.destination_record_id): {
            "website": value
        }},
    )


def test_separate_connections_claim_one_reconciliation_job(
    tmp_path, sync_issue, fixed_time
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    repository.schedule_reconciliation(target, fixed_time, "concurrent")
    barrier = Barrier(3)

    def claim(worker):
        separate = IncidentRepository(repository.path)
        barrier.wait()
        return separate.claim_reconciliation(worker, fixed_time)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim, f"worker-{index}") for index in range(2)]
        barrier.wait()
        claims = [future.result() for future in futures]

    assert sum(claim is not None for claim in claims) == 1


def test_lease_token_fences_reused_worker_identity(
    tmp_path, sync_issue, fixed_time
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    incident = discover(repository, target, simulator, fixed_time)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "approval", fixed_time
    )
    stale = repository.claim_recovery("same-worker", fixed_time, lease_seconds=1)
    later = fixed_time + timedelta(seconds=2)
    repository.recover_expired_leases(later, "restart")
    current = repository.claim_verification("same-worker", later)

    assert stale.lease_token != current.lease_token
    with pytest.raises(LeaseConflict):
        repository.reject_stale_recovery(stale, later, "stale callback")
    repository.finish_verification(
        current, later, converged=False, reason="issue remains open"
    )
    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW


def test_separate_connections_claim_one_recovery_job(
    tmp_path, sync_issue, fixed_time
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    incident = discover(repository, target, simulator, fixed_time)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "approval", fixed_time
    )
    barrier = Barrier(3)

    def claim(worker):
        separate = IncidentRepository(repository.path)
        barrier.wait()
        return separate.claim_recovery(worker, fixed_time)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim, f"recovery-{index}") for index in range(2)]
        barrier.wait()
        claims = [future.result() for future in futures]

    assert sum(claim is not None for claim in claims) == 1


def test_http_gateway_handles_empty_exact_multiple_and_optional_fields(
    issue_payload, fixed_time
):
    minimal_one = SyncIssue.model_validate({
        key: value for key, value in issue_payload.items()
        if key not in {"destination_table_id", "destination_table_name", "changed_columns"}
    })
    minimal_two = SyncIssue.model_validate({
        **minimal_one.model_dump(),
        "issue_id": "00000000-0000-4000-8000-000000000123",
        "source_record_id": "source-2",
    })
    simulator = SimulatedStacksync([minimal_one, minimal_two])
    gateway = HttpStacksyncGateway(
        "http://testserver",
        client=TestClient(create_simulated_stacksync(simulator=simulator)),
    )
    target = SyncTarget(42, "usnv", minimal_one.base_id)

    first = gateway.list_issues(target, page_number=0, page_size=1)
    second = gateway.list_issues(target, page_number=first.pagination.next_page, page_size=1)
    empty = gateway.list_issues(
        target, page_number=0, page_size=1,
        issue_id="00000000-0000-4000-8000-000000000999",
    )

    assert first.pagination.next_page == 1
    assert second.pagination.next_page is None
    assert first.pagination.total_pages == 2  # documented floor(total/page_size)
    assert empty.data == []
    assert empty.pagination.total_pages is None
    assert first.data[0].changed_columns is None


def test_malformed_http_page_is_isolated_and_backed_off(
    tmp_path, sync_issue, fixed_time
):
    app = FastAPI()

    @app.get("/v1/workspaces/{workspace_id}/issues")
    def malformed(workspace_id: int):
        return {"unexpected": workspace_id}

    gateway = HttpStacksyncGateway(
        "http://testserver", client=TestClient(app)
    )
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    repository.schedule_reconciliation(target, fixed_time, "malformed")

    assert ReconciliationEngine(repository, gateway).run_once("worker", fixed_time)
    assert repository.claim_reconciliation("early", fixed_time) is None
    assert repository.claim_reconciliation(
        "later", fixed_time + timedelta(seconds=1)
    ) is not None


class DefiniteFailure(SimulatedStacksync):
    def retry_issue(self, target, issue_id):
        self.retry_calls.append(issue_id)
        return ResolveIssuesResponse(data=ResolvedSyncIssues(
            ids=[], success_issue_count=0, failed_issue_count=1,
            error_list=[{"issue_id": issue_id, "error": "rejected"}],
        ))


def test_definite_retry_failure_goes_directly_to_manual_review(
    tmp_path, sync_issue, fixed_time
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    simulator = DefiniteFailure([sync_issue])
    incident = discover(repository, target, simulator, fixed_time)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "reject", fixed_time
    )

    RecoveryEngine(repository, simulator).run_recovery_once("worker", fixed_time)

    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW
    job = repository.recovery_job(repository.audit_log()[-1]["job_id"])
    assert job.status == "unresolved"
    assert job.ambiguous is False


def test_stale_and_repeated_approvals_are_explicit_conflicts(
    tmp_path, sync_issue, fixed_time
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    incident = discover(repository, target, simulator, fixed_time)
    with pytest.raises(StaleOccurrence):
        repository.approve_retry(
            incident.id, "not-current", "operator", "stale", fixed_time
        )
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "first", fixed_time
    )
    with pytest.raises(RecoveryApprovalConflict):
        repository.approve_retry(
            incident.id, incident.latest_issue_id, "operator", "repeat", fixed_time
        )

    client = TestClient(create_app(
        repository, webhook_bearer_secret="secret", clock=lambda: fixed_time
    ))
    response = client.post(
        f"/incidents/{incident.id}/approve-retry",
        json={
            "expected_issue_id": incident.latest_issue_id,
            "approved_by": "operator",
        },
    )
    assert response.status_code == 409


def test_startup_requires_explicit_webhook_secret(tmp_path, monkeypatch):
    from syncsentinel.main import create_application

    monkeypatch.setenv("SYNCSENTINEL_DB", str(tmp_path / "startup.sqlite3"))
    monkeypatch.delenv("STACKSYNC_WEBHOOK_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="STACKSYNC_WEBHOOK_SECRET"):
        create_application()


def test_polling_reopens_resolved_incident_when_same_issue_is_open_again(
    tmp_path, sync_issue, fixed_time
):
    source, destination = stores(sync_issue)
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync(
        [sync_issue], source_records=source, destination_records=destination
    )
    incident = discover(repository, target, simulator, fixed_time)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "resolve", fixed_time
    )
    engine = RecoveryEngine(repository, simulator)
    engine.run_recovery_once("worker", fixed_time)
    engine.run_verification_once("verifier", fixed_time)
    assert repository.get(incident.id).state is IncidentState.RESOLVED

    simulator.issues[sync_issue.issue_id] = sync_issue
    later = fixed_time + timedelta(minutes=1)
    repository.schedule_reconciliation(target, later, "reopen")
    ReconciliationEngine(repository, simulator).run_once("worker", later)

    assert repository.get(incident.id).state is IncidentState.DISCOVERED
    assert "incident_reopened" in {
        event["event_type"] for event in repository.audit_log()
    }


class TransientVerification(SimulatedStacksync):
    def read_record(self, side, app_id, table_id, record_id):
        raise TransientAPIError("temporary connector read failure")


def test_transient_verification_is_bounded(
    tmp_path, sync_issue, fixed_time
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    simulator = TransientVerification([sync_issue])
    incident = discover(repository, target, simulator, fixed_time)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "bounded", fixed_time
    )
    engine = RecoveryEngine(repository, simulator)
    engine.run_recovery_once("worker", fixed_time)
    for offset in (0, 1, 3):
        engine.run_verification_once(
            "verifier", fixed_time + timedelta(seconds=offset)
        )

    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW


def test_empty_fields_fail_closed_after_successful_retry_and_issue_closure(
    tmp_path, issue_payload, fixed_time
):
    issue = SyncIssue.model_validate({**issue_payload, "changed_columns": {}})
    source = {
        (issue.destination_app_id, issue.destination_table_id, issue.source_record_id): {
            "website": "source-value"
        }
    }
    destination = {
        (issue.destination_app_id, issue.destination_table_id, issue.destination_record_id): {
            "website": "different-destination-value"
        }
    }
    repository, target = setup_repository(tmp_path, issue, fixed_time)
    simulator = SimulatedStacksync(
        [issue], source_records=source, destination_records=destination
    )
    incident = discover(repository, target, simulator, fixed_time)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "empty-fields", fixed_time
    )
    engine = RecoveryEngine(repository, simulator)

    engine.run_recovery_once("recovery", fixed_time)
    assert issue.issue_id not in simulator.issues
    engine.run_verification_once("verification", fixed_time)

    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW


def test_continuous_tick_recovers_expired_lease_without_process_restart(
    tmp_path, sync_issue, fixed_time, monkeypatch
):
    repository, target = setup_repository(tmp_path, sync_issue, fixed_time)
    repository.schedule_reconciliation(target, fixed_time, "expired")
    claimed = repository.claim_reconciliation(
        "long-lived-worker", fixed_time, lease_seconds=1
    )
    assert claimed is not None
    simulator = SimulatedStacksync([sync_issue])
    later = fixed_time + timedelta(seconds=2)
    monkeypatch.setattr("syncsentinel.worker.utc_now", lambda: later)

    run_tick("long-lived-worker", repository, simulator)

    incident = repository.get(
        Incident.from_observation(sync_issue.to_observation()).id
    )
    assert incident is not None
    assert "lease_recovered" in {
        event["event_type"] for event in repository.audit_log()
    }
