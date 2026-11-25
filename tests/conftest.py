from datetime import UTC, datetime

import pytest

from syncsentinel.contracts import SyncIssue
from syncsentinel.domain import IssueObservation


@pytest.fixture
def issue_payload() -> dict[str, object]:
    return {
        "issue_id": "4c5d6e7f-8a9b-4c0d-9e1f-2a3b4c5d6e7f",
        "base_id": "3f6c1d2e-8a4b-4c7d-9e21-5b6f7a8c9d01",
        "destination_app_id": "b71d04e5",
        "destination_app_type": "postgres",
        "destination_table_id": "accounts",
        "destination_table_name": "accounts",
        "action": "update",
        "issue_message": "value too long for type character varying(80)",
        "changed_columns": {"website": "https://example.invalid/too-long"},
        "source_record_id": "001Hs00001ZXkQ9IAL",
        "destination_record_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
        "created_time": "2026-10-01T09:12:44",
        "last_modified_time": "2026-10-01T09:12:44",
    }


@pytest.fixture
def sync_issue(issue_payload: dict[str, object]) -> SyncIssue:
    return SyncIssue.model_validate(issue_payload)


@pytest.fixture
def observation(sync_issue: SyncIssue) -> IssueObservation:
    return sync_issue.to_observation()


@pytest.fixture
def fixed_time() -> datetime:
    return datetime(2026, 11, 25, 12, 0, tzinfo=UTC)
