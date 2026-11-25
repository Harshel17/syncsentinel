from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from syncsentinel.contracts import (
    ListIssuesResponse,
    Pagination,
    ResolveIssuesRequest,
    ResolveIssuesResponse,
    ResolvedSyncIssues,
    SyncIssue,
)
from syncsentinel.persistence import SyncTarget


class SimulatedStacksync:
    """Deterministic test double for verified issue APIs and simulated record reads."""

    def __init__(
        self,
        issues: Iterable[SyncIssue] = (),
        *,
        source_records: dict[tuple[str, str, str], dict[str, Any]] | None = None,
        destination_records: dict[tuple[str, str, str], dict[str, Any]] | None = None,
        converge_on_retry: bool = True,
    ):
        self.issues = {issue.issue_id: issue for issue in issues}
        self.source_records = source_records or {}
        self.destination_records = destination_records or {}
        self.converge_on_retry = converge_on_retry
        self.resolution_mode = "normal"
        self.retry_calls: list[str] = []
        self.list_calls: list[int] = []

    def list_issues(
        self,
        target: SyncTarget,
        *,
        page_number: int,
        page_size: int,
        app_id: str | None = None,
        table_id: str | None = None,
        issue_id: str | None = None,
    ) -> ListIssuesResponse:
        self.list_calls.append(page_number)
        matches = [
            issue
            for issue in self.issues.values()
            if issue.base_id == target.base_id
            and (app_id is None or issue.destination_app_id == app_id)
            and (table_id is None or issue.destination_table_id == table_id)
            and (issue_id is None or issue.issue_id == issue_id)
        ]
        matches.sort(key=lambda item: item.last_modified_time, reverse=True)
        start = page_number * page_size
        data = matches[start : start + page_size]
        next_page = page_number + 1 if start + len(data) < len(matches) else None
        total_pages = len(matches) // page_size if matches else None
        return ListIssuesResponse(
            data=data,
            pagination=Pagination(
                current_page=page_number,
                next_page=next_page,
                total_pages=total_pages,
            ),
            total_number_of_issues=len(matches),
        )

    def retry_issue(self, target: SyncTarget, issue_id: str) -> ResolveIssuesResponse:
        del target
        self.retry_calls.append(issue_id)
        issue = self.issues.get(issue_id)
        if issue is None:
            return ResolveIssuesResponse(
                data=ResolvedSyncIssues(
                    ids=[], success_issue_count=0, failed_issue_count=1,
                    error_list=[{"issue_id": issue_id, "error": "not found"}],
                )
            )
        if self.converge_on_retry:
            source_key = (
                issue.destination_app_id,
                issue.destination_table_id or "<unknown-table>",
                issue.source_record_id or "",
            )
            destination_key = (
                issue.destination_app_id,
                issue.destination_table_id or "<unknown-table>",
                issue.destination_record_id or "",
            )
            source = self.source_records.get(source_key, {})
            destination = self.destination_records.setdefault(destination_key, {})
            for field in issue.changed_columns or {}:
                if field in source:
                    destination[field] = source[field]
        del self.issues[issue_id]
        return ResolveIssuesResponse(
            data=ResolvedSyncIssues(
                ids=[issue_id], success_issue_count=1, failed_issue_count=0,
                error_list=[],
            )
        )

    def read_record(
        self, side: str, app_id: str, table_id: str, record_id: str
    ) -> dict[str, Any]:
        store = self.source_records if side == "source" else self.destination_records
        key = (app_id, table_id, record_id)
        if key not in store:
            raise KeyError(key)
        return dict(store[key])

    def clear(self) -> None:
        self.issues.clear()
        self.source_records.clear()
        self.destination_records.clear()
        self.retry_calls.clear()
        self.list_calls.clear()
        self.converge_on_retry = True
        self.resolution_mode = "normal"

    def configure_scenario(self, name: str) -> None:
        if name not in {
            "missed_webhook",
            "verified_recovery",
            "false_success",
            "ambiguous_outcome",
        }:
            raise ValueError(f"unknown scenario: {name}")
        self.clear()
        issue = demo_issue()
        self.issues[issue.issue_id] = issue
        self.source_records[(
            issue.destination_app_id,
            issue.destination_table_id or "<unknown-table>",
            issue.source_record_id or "",
        )] = {"website": "https://syncsentinel.dev/current"}
        self.destination_records[(
            issue.destination_app_id,
            issue.destination_table_id or "<unknown-table>",
            issue.destination_record_id or "",
        )] = {"website": "https://syncsentinel.dev/stale"}
        if name == "false_success":
            self.converge_on_retry = False
        if name == "ambiguous_outcome":
            self.resolution_mode = "ambiguous_after_apply"


def create_simulated_stacksync(
    issues: Iterable[SyncIssue] = (),
    *,
    simulator: SimulatedStacksync | None = None,
) -> FastAPI:
    """Expose the verified issue routes and explicit simulator-only record routes."""

    state = simulator or SimulatedStacksync(issues)
    app = FastAPI(title="Simulated Stacksync Issues API")
    app.state.simulator = state

    @app.get(
        "/v1/workspaces/{workspace_id}/issues",
        response_model=ListIssuesResponse,
        response_model_exclude_none=True,
    )
    def list_issues(
        workspace_id: int,
        region: str,
        base_id: str | None = None,
        sync_id: str | None = None,
        app_id: str | None = None,
        table_id: str | None = None,
        issue_id: str | None = None,
        page_number: int = Query(0, ge=0),
        page_size: int = Query(100, ge=1, le=1000),
    ) -> ListIssuesResponse:
        selected_base = base_id or sync_id
        if selected_base is None:
            # The real route permits workspace-wide reads. The simulator groups its
            # fixture around one base, so use the issue's base when unambiguous.
            bases = {issue.base_id for issue in state.issues.values()}
            selected_base = next(iter(bases), "") if len(bases) <= 1 else ""
        return state.list_issues(
            SyncTarget(workspace_id, region, selected_base),
            page_number=page_number,
            page_size=page_size,
            app_id=app_id,
            table_id=table_id,
            issue_id=issue_id,
        )

    @app.post(
        "/v1/workspaces/{workspace_id}/issues/action",
        response_model=ResolveIssuesResponse,
    )
    def resolve_issues(
        workspace_id: int, region: str, request: ResolveIssuesRequest
    ) -> ResolveIssuesResponse:
        succeeded: list[str] = []
        failed: list[str] = []
        for value in request.ids:
            result = state.retry_issue(SyncTarget(workspace_id, region, ""), value)
            (succeeded if result.data.success_issue_count else failed).append(value)
        if state.resolution_mode == "ambiguous_after_apply":
            return JSONResponse(
                status_code=504,
                content={"error": "simulated timeout after applying recovery"},
            )
        return ResolveIssuesResponse(
            data=ResolvedSyncIssues(
                ids=succeeded,
                success_issue_count=len(succeeded),
                failed_issue_count=len(failed),
                error_list=[{"issue_id": value, "error": "not found"} for value in failed],
            )
        )

    @app.get("/simulator/apps/{app_id}/tables/{table_id}/{side}/{record_id}")
    def read_simulated_record(
        app_id: str, table_id: str, side: str, record_id: str
    ) -> dict[str, Any]:
        if side not in {"source", "destination"}:
            raise HTTPException(status_code=400, detail="invalid side")
        try:
            return {"record": state.read_record(side, app_id, table_id, record_id)}
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="record not found") from exc

    @app.post("/simulator/control/scenarios/{scenario}")
    def configure_scenario(scenario: str) -> dict[str, str]:
        try:
            state.configure_scenario(scenario)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "ready", "scenario": scenario}

    @app.post("/simulator/control/reset")
    def reset_simulator() -> dict[str, str]:
        state.clear()
        return {"status": "reset"}

    return app


def demo_issue() -> SyncIssue:
    return SyncIssue.model_validate(
        {
            "issue_id": "4c5d6e7f-8a9b-4c0d-9e1f-2a3b4c5d6e7f",
            "base_id": "3f6c1d2e-8a4b-4c7d-9e21-5b6f7a8c9d01",
            "destination_app_id": "b71d04e5",
            "destination_app_type": "postgres",
            "destination_table_id": "accounts",
            "destination_table_name": "accounts",
            "action": "update",
            "issue_message": "value too long for type character varying(80)",
            "changed_columns": {"website": "https://syncsentinel.dev/current"},
            "source_record_id": "source-account-1",
            "destination_record_id": "destination-account-1",
            "created_time": "2026-11-25T12:00:00",
            "last_modified_time": "2026-11-25T12:00:00",
        }
    )


def create_demo_simulator() -> FastAPI:
    """Start an empty simulator; the console loads explicit scenarios."""
    simulator = SimulatedStacksync()
    return create_simulated_stacksync(simulator=simulator)
