from dataclasses import replace

import pytest

from syncsentinel.domain import IncidentState
from syncsentinel.persistence import IncidentRepository


@pytest.fixture
def repository(tmp_path):
    repository = IncidentRepository(tmp_path / "test.sqlite3")
    repository.initialize()
    return repository


def test_observation_and_audit_event_are_committed_together(
    repository, observation, fixed_time
):
    incident, created = repository.record_observation(observation, fixed_time)

    assert created is True
    assert repository.get(incident.id) == incident
    history = repository.history(incident.id)
    assert [event["event_type"] for event in history] == ["incident_discovered"]


def test_duplicate_issue_id_is_idempotent(repository, observation, fixed_time):
    first, first_created = repository.record_observation(observation, fixed_time)
    second, second_created = repository.record_observation(observation, fixed_time)

    assert first_created is True
    assert second_created is False
    assert first == second
    assert len(repository.history(first.id)) == 1


def test_new_issue_id_for_same_record_updates_existing_incident(
    repository, observation, fixed_time
):
    first, _ = repository.record_observation(observation, fixed_time)
    recurrence = replace(observation, issue_id="00000000-0000-4000-8000-000000000002")

    second, created = repository.record_observation(recurrence, fixed_time)

    assert created is True
    assert second.id == first.id
    assert second.latest_issue_id == recurrence.issue_id
    assert [event["event_type"] for event in repository.history(first.id)] == [
        "incident_discovered",
        "issue_refreshed",
    ]


def test_transition_uses_optimistic_version_and_writes_history(
    repository, observation, fixed_time
):
    incident, _ = repository.record_observation(observation, fixed_time)
    updated = repository.transition(
        incident.id,
        IncidentState.AWAITING_APPROVAL,
        "policy requires approval",
        fixed_time,
        expected_version=incident.version,
    )

    assert updated.version == 2
    assert repository.history(incident.id)[-1]["reason"] == "policy requires approval"
    with pytest.raises(RuntimeError, match="concurrently"):
        repository.transition(
            incident.id,
            IncidentState.APPROVED,
            "stale worker",
            fixed_time,
            expected_version=incident.version,
        )
