from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, uuid5


def utc_now() -> datetime:
    return datetime.now(UTC)


class IncidentState(StrEnum):
    DISCOVERED = "discovered"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    EXECUTING = "executing"
    VERIFYING = "verifying"
    RESOLVED = "resolved"
    MANUAL_REVIEW = "manual_review"


class RecoveryAction(StrEnum):
    RETRY = "retry"
    REVERT = "revert"
    IGNORE = "ignore"


ALLOWED_TRANSITIONS: dict[IncidentState, frozenset[IncidentState]] = {
    IncidentState.DISCOVERED: frozenset(
        {IncidentState.AWAITING_APPROVAL, IncidentState.MANUAL_REVIEW}
    ),
    IncidentState.AWAITING_APPROVAL: frozenset(
        {IncidentState.APPROVED, IncidentState.MANUAL_REVIEW}
    ),
    IncidentState.APPROVED: frozenset(
        {IncidentState.EXECUTING, IncidentState.MANUAL_REVIEW}
    ),
    IncidentState.EXECUTING: frozenset(
        {IncidentState.VERIFYING, IncidentState.MANUAL_REVIEW}
    ),
    IncidentState.VERIFYING: frozenset(
        {IncidentState.RESOLVED, IncidentState.MANUAL_REVIEW}
    ),
    IncidentState.MANUAL_REVIEW: frozenset(
        {IncidentState.AWAITING_APPROVAL, IncidentState.RESOLVED}
    ),
    IncidentState.RESOLVED: frozenset({IncidentState.DISCOVERED}),
}


class InvalidTransition(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class IssueObservation:
    issue_id: str
    base_id: str
    destination_app_id: str
    destination_table_id: str | None
    source_record_id: str | None
    destination_record_id: str | None
    action: str
    issue_message: str
    changed_columns: dict[str, Any] | None
    created_time: str
    last_modified_time: str

    @property
    def record_identity(self) -> str:
        record_id = self.source_record_id or self.destination_record_id
        if not record_id:
            # The public response permits nullable record ids. Hashing the immutable
            # observation gives the simulator a deterministic conservative fallback.
            record_id = hashlib.sha256(
                json.dumps(
                    {
                        "issue_id": self.issue_id,
                        "changed_columns": self.changed_columns,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        return record_id

    @property
    def fingerprint(self) -> str:
        parts = (
            self.base_id,
            self.destination_app_id,
            self.destination_table_id or "<unknown-table>",
            self.record_identity,
        )
        return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


@dataclass(slots=True)
class Incident:
    id: str
    fingerprint: str
    latest_issue_id: str
    base_id: str
    destination_app_id: str
    destination_table_id: str
    record_identity: str
    state: IncidentState = IncidentState.DISCOVERED
    recovery_action: RecoveryAction | None = None
    version: int = 1
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    @classmethod
    def from_observation(cls, issue: IssueObservation, now: datetime | None = None) -> Incident:
        timestamp = now or utc_now()
        incident_id = str(uuid5(NAMESPACE_URL, f"syncsentinel:incident:{issue.fingerprint}"))
        return cls(
            id=incident_id,
            fingerprint=issue.fingerprint,
            latest_issue_id=issue.issue_id,
            base_id=issue.base_id,
            destination_app_id=issue.destination_app_id,
            destination_table_id=issue.destination_table_id or "<unknown-table>",
            record_identity=issue.record_identity,
            created_at=timestamp,
            updated_at=timestamp,
        )

    def transition(self, target: IncidentState, now: datetime | None = None) -> None:
        if target not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidTransition(f"cannot transition from {self.state} to {target}")
        self.state = target
        self.version += 1
        self.updated_at = now or utc_now()


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    """A deny-by-default policy; explicit approval is always safe."""

    automatically_allowed_actions: frozenset[RecoveryAction] = frozenset()

    def permits_automatic(self, action: RecoveryAction) -> bool:
        return action in self.automatically_allowed_actions
