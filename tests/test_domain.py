import pytest

from syncsentinel.domain import (
    Incident,
    IncidentState,
    InvalidTransition,
    RecoveryAction,
    RecoveryPolicy,
)


def test_incident_identity_is_deterministic(observation, fixed_time):
    first = Incident.from_observation(observation, fixed_time)
    second = Incident.from_observation(observation, fixed_time)

    assert first.id == second.id
    assert first.fingerprint == second.fingerprint


def test_state_machine_rejects_skipping_policy_gate(observation, fixed_time):
    incident = Incident.from_observation(observation, fixed_time)

    with pytest.raises(InvalidTransition):
        incident.transition(IncidentState.EXECUTING, fixed_time)


def test_happy_path_requires_verification_before_resolved(observation, fixed_time):
    incident = Incident.from_observation(observation, fixed_time)
    for state in (
        IncidentState.AWAITING_APPROVAL,
        IncidentState.APPROVED,
        IncidentState.EXECUTING,
        IncidentState.VERIFYING,
        IncidentState.RESOLVED,
    ):
        incident.transition(state, fixed_time)

    assert incident.state is IncidentState.RESOLVED
    assert incident.version == 6


def test_recovery_policy_denies_automation_by_default():
    assert not RecoveryPolicy().permits_automatic(RecoveryAction.RETRY)
    policy = RecoveryPolicy(frozenset({RecoveryAction.RETRY}))
    assert policy.permits_automatic(RecoveryAction.RETRY)
    assert not policy.permits_automatic(RecoveryAction.REVERT)
