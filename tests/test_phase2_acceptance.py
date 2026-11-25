from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from fastapi.testclient import TestClient

from syncsentinel.app import create_app
from syncsentinel.contracts import SyncIssue
from syncsentinel.domain import Incident, IncidentState
from syncsentinel.gateway import AmbiguousOutcome, TransientAPIError
from syncsentinel.persistence import IncidentRepository, SyncTarget
from syncsentinel.services import ReconciliationEngine, RecoveryEngine
from syncsentinel.simulator import SimulatedStacksync


def configured_repository(tmp_path, sync_issue, fixed_time):
    repository = IncidentRepository(tmp_path / "phase2.sqlite3")
    repository.initialize()
    target = SyncTarget(42, "usnv", sync_issue.base_id, poll_interval_seconds=60)
    repository.register_target(target, fixed_time)
    return repository, target


def record_stores(sync_issue, source_value="fixed", destination_value="old"):
    source_key = (
        sync_issue.destination_app_id,
        sync_issue.destination_table_id,
        sync_issue.source_record_id,
    )
    destination_key = (
        sync_issue.destination_app_id,
        sync_issue.destination_table_id,
        sync_issue.destination_record_id,
    )
    return (
        {source_key: {"website": source_value, "untouched": 1}},
        {destination_key: {"website": destination_value, "untouched": 1}},
    )


def discover(repository, target, simulator, fixed_time, *, page_size=100):
    repository.schedule_reconciliation(
        target, fixed_time, "test-discovery", reason="scheduled_poll"
    )
    assert ReconciliationEngine(
        repository, simulator, page_size=page_size
    ).run_once("reconciler-1", fixed_time)


def incident_for(repository, sync_issue):
    expected = sync_issue.to_observation()
    incident_id = Incident.from_observation(expected).id
    return repository.get(incident_id)


def webhook_payload(sync_issue, created_time="2026-11-25T12:00:00"):
    return {
        "notification": {
            "created_time": created_time,
            "notification_type": "real_time_issue_notification",
            "msg": "Some records had issues syncing",
            "base_id": sync_issue.base_id,
            "app_id": sync_issue.destination_app_id,
            "internal_table_id": sync_issue.destination_table_id,
        }
    }


def test_missed_webhook_is_recovered_by_scheduled_polling(
    tmp_path, sync_issue, fixed_time
):
    repository, _ = configured_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])

    assert repository.schedule_due_targets(fixed_time) == 1
    assert ReconciliationEngine(repository, simulator).run_once("worker", fixed_time)

    incident = incident_for(repository, sync_issue)
    assert incident is not None
    assert incident.latest_issue_id == sync_issue.issue_id
    assert "webhook_received" not in {
        event["event_type"] for event in repository.audit_log()
    }


def test_duplicate_webhooks_coalesce_and_do_not_duplicate_incidents(
    tmp_path, sync_issue, fixed_time
):
    repository, _ = configured_repository(tmp_path, sync_issue, fixed_time)
    app = create_app(
        repository, webhook_bearer_secret="local-secret", clock=lambda: fixed_time
    )
    client = TestClient(app)
    headers = {"Authorization": "Bearer local-secret", "X-Correlation-Id": "burst"}

    first = client.post("/webhooks/stacksync", json=webhook_payload(sync_issue), headers=headers)
    second = client.post("/webhooks/stacksync", json=webhook_payload(sync_issue), headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json()["scheduled"] is True
    assert second.json()["scheduled"] is False
    simulator = SimulatedStacksync([sync_issue])
    ReconciliationEngine(repository, simulator).run_once("worker", fixed_time)
    # Repeated polling of the same occurrence is idempotent too.
    repository.schedule_reconciliation(
        repository.target_for_base(sync_issue.base_id), fixed_time, "again"
    )
    ReconciliationEngine(repository, simulator).run_once("worker", fixed_time)
    incident = incident_for(repository, sync_issue)
    assert incident.version == 1
    assert {e["event_type"] for e in repository.audit_log()} >= {
        "notification_coalesced",
        "duplicate_suppressed",
    }


def test_out_of_order_webhook_hint_cannot_regress_incident_state(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "approval", fixed_time
    )

    client = TestClient(create_app(
        repository, webhook_bearer_secret="secret", clock=lambda: fixed_time
    ))
    response = client.post(
        "/webhooks/stacksync",
        json=webhook_payload(sync_issue, "2020-01-01T00:00:00"),
        headers={"Authorization": "Bearer secret"},
    )
    assert response.status_code == 202
    ReconciliationEngine(repository, simulator).run_once("worker", fixed_time)

    assert repository.get(incident.id).state is IncidentState.APPROVED


def test_concurrent_workers_cannot_claim_same_jobs(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    repository.schedule_reconciliation(target, fixed_time, "claim")
    first = repository.claim_reconciliation("worker-a", fixed_time)
    second = repository.claim_reconciliation("worker-b", fixed_time)
    assert first is not None
    assert second is None

    # Complete discovery, then prove recovery claims have the same exclusion.
    repository.finish_reconciliation(first, fixed_time)
    discover(repository, target, simulator, fixed_time)
    recovery = repository.approve_retry(
        incident_for(repository, sync_issue).id,
        sync_issue.issue_id,
        "operator", "recovery", fixed_time,
    )
    assert recovery.status == "pending"
    assert repository.claim_recovery("worker-a", fixed_time) is not None
    assert repository.claim_recovery("worker-b", fixed_time) is None


def test_stale_issue_occurrence_cannot_be_recovered(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "stale-test", fixed_time
    )

    newer = SyncIssue.model_validate({
        **sync_issue.model_dump(),
        "issue_id": "00000000-0000-4000-8000-000000000099",
        "last_modified_time": "2026-11-25T12:01:00",
    })
    simulator.issues = {newer.issue_id: newer}
    repository.record_observation(newer.to_observation(), fixed_time, "newer")

    assert RecoveryEngine(repository, simulator).run_recovery_once("recovery", fixed_time)
    assert simulator.retry_calls == []
    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW


class AmbiguousSimulator(SimulatedStacksync):
    def retry_issue(self, target, issue_id):
        self.retry_calls.append(issue_id)
        raise AmbiguousOutcome("simulated timeout after request send")


def test_ambiguous_response_never_triggers_blind_retry(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    source, destination = record_stores(sync_issue)
    simulator = AmbiguousSimulator(
        [sync_issue], source_records=source, destination_records=destination
    )
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    job = repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "ambiguous", fixed_time
    )
    engine = RecoveryEngine(repository, simulator)

    assert engine.run_recovery_once("worker", fixed_time)
    assert engine.run_recovery_once("worker", fixed_time) is False
    assert simulator.retry_calls == [sync_issue.issue_id]
    persisted = repository.recovery_job(job.id)
    assert persisted.status == "verify_pending"
    assert persisted.ambiguous is True


def test_successful_api_response_with_mismatch_remains_unresolved(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    source, destination = record_stores(sync_issue)
    simulator = SimulatedStacksync(
        [sync_issue], source_records=source, destination_records=destination,
        converge_on_retry=False,
    )
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "mismatch", fixed_time
    )
    engine = RecoveryEngine(repository, simulator)

    engine.run_recovery_once("recovery", fixed_time)
    assert repository.get(incident.id).state is IncidentState.VERIFYING
    engine.run_verification_once("verifier", fixed_time)

    assert repository.get(incident.id).state is IncidentState.MANUAL_REVIEW
    assert repository.audit_log()[-1]["event_type"] == "verification_failed"


def test_verified_convergence_resolves_incident(tmp_path, sync_issue, fixed_time):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    source, destination = record_stores(sync_issue)
    simulator = SimulatedStacksync(
        [sync_issue], source_records=source, destination_records=destination,
        converge_on_retry=True,
    )
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "converge", fixed_time
    )
    engine = RecoveryEngine(repository, simulator)

    engine.run_recovery_once("recovery", fixed_time)
    assert repository.get(incident.id).state is IncidentState.VERIFYING
    engine.run_verification_once("verifier", fixed_time)

    assert repository.get(incident.id).state is IncidentState.RESOLVED
    assert repository.audit_log()[-1]["event_type"] == "verification_succeeded"


def test_worker_crash_does_not_lose_persisted_recovery_intent(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue])
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    intent = repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "durable", fixed_time
    )

    restarted = IncidentRepository(repository.path)
    restarted.initialize()
    claimed = restarted.claim_recovery("new-process", fixed_time)

    assert claimed.id == intent.id
    assert claimed.issue_id == sync_issue.issue_id


def test_restart_recovers_expired_execution_lease_without_reexecution(
    tmp_path, sync_issue, fixed_time
):
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    source, destination = record_stores(
        sync_issue, source_value="already-applied", destination_value="already-applied"
    )
    simulator = SimulatedStacksync(
        [sync_issue], source_records=source, destination_records=destination
    )
    discover(repository, target, simulator, fixed_time)
    incident = incident_for(repository, sync_issue)
    repository.approve_retry(
        incident.id, incident.latest_issue_id, "operator", "restart", fixed_time
    )
    assert repository.claim_recovery("crashed-worker", fixed_time, lease_seconds=30)

    restarted = IncidentRepository(repository.path)
    restarted.initialize()
    later = fixed_time + timedelta(seconds=31)
    recovered = restarted.recover_expired_leases(later, "process-restart")
    assert recovered == 1
    restarted.record_worker_started("new-worker", later, recovered)
    engine = RecoveryEngine(restarted, simulator)
    assert engine.run_recovery_once("new-worker", later) is False
    assert engine.run_verification_once("new-verifier", later)

    assert simulator.retry_calls == []
    assert restarted.get(incident.id).state is IncidentState.MANUAL_REVIEW
    events = {event["event_type"] for event in restarted.audit_log()}
    assert {"worker_restarted", "lease_recovered"} <= events


def test_reconciliation_follows_pagination_and_uses_bounded_backoff(
    tmp_path, sync_issue, fixed_time
):
    second = SyncIssue.model_validate({
        **sync_issue.model_dump(),
        "issue_id": "00000000-0000-4000-8000-000000000077",
        "source_record_id": "source-2",
        "destination_record_id": "destination-2",
    })
    repository, target = configured_repository(tmp_path, sync_issue, fixed_time)
    simulator = SimulatedStacksync([sync_issue, second])
    discover(repository, target, simulator, fixed_time, page_size=1)
    assert simulator.list_calls == [0, 1]

    class FailingReads(SimulatedStacksync):
        def list_issues(self, *args, **kwargs):
            raise TransientAPIError("temporary read failure")

    repository.schedule_reconciliation(target, fixed_time, "backoff")
    engine = ReconciliationEngine(repository, FailingReads(), max_attempts=2)
    assert engine.run_once("worker", fixed_time)
    assert repository.claim_reconciliation("too-early", fixed_time) is None
    assert repository.claim_reconciliation(
        "after-backoff", fixed_time + timedelta(seconds=1)
    ) is not None
