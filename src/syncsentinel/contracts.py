from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from syncsentinel.domain import IssueObservation, RecoveryAction


class WebhookNotification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    created_time: str
    notification_type: Literal[
        "real_time_issue_notification", "base_activity", "backfill"
    ]
    msg: str
    base_id: str
    app_id: str | None = None
    internal_table_id: str | None = None


class WebhookEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    notification: WebhookNotification


class SyncIssue(BaseModel):
    model_config = ConfigDict(extra="allow")

    issue_id: str
    base_id: str
    destination_app_id: str
    destination_app_type: str
    destination_table_id: str | None = None
    destination_table_name: str | None = None
    action: str
    issue_message: str
    changed_columns: dict[str, Any] | None = None
    source_record_id: str | None = None
    destination_record_id: str | None = None
    created_time: str
    last_modified_time: str

    def to_observation(self) -> IssueObservation:
        return IssueObservation(**self.model_dump(include=set(IssueObservation.__dataclass_fields__)))


class Pagination(BaseModel):
    current_page: int
    next_page: int | None = None
    total_pages: int | None = None


class ListIssuesResponse(BaseModel):
    data: list[SyncIssue]
    pagination: Pagination
    total_number_of_issues: int


class ResolveIssuesRequest(BaseModel):
    ids: list[str] = Field(min_length=1)
    action: RecoveryAction


class ResolvedSyncIssues(BaseModel):
    ids: list[str]
    success_issue_count: int
    failed_issue_count: int
    error_list: list[Any]


class ResolveIssuesResponse(BaseModel):
    data: ResolvedSyncIssues
