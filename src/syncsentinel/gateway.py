from __future__ import annotations

from typing import Any, Protocol

import httpx
from pydantic import ValidationError

from syncsentinel.contracts import ListIssuesResponse, ResolveIssuesResponse
from syncsentinel.persistence import SyncTarget


class TransientAPIError(RuntimeError):
    """A read failed before a useful response and may be scheduled again."""


class AmbiguousOutcome(RuntimeError):
    """A write may have reached the remote system; it must not be retried blindly."""


class MissingRecord(RuntimeError):
    pass


class ActionRejected(RuntimeError):
    pass


class MalformedAPIResponse(RuntimeError):
    pass


class StacksyncGateway(Protocol):
    def list_issues(
        self,
        target: SyncTarget,
        *,
        page_number: int,
        page_size: int,
        app_id: str | None = None,
        table_id: str | None = None,
        issue_id: str | None = None,
    ) -> ListIssuesResponse: ...

    def retry_issue(self, target: SyncTarget, issue_id: str) -> ResolveIssuesResponse: ...

    def read_record(
        self,
        side: str,
        app_id: str,
        table_id: str,
        record_id: str,
    ) -> dict[str, Any]: ...


class HttpStacksyncGateway:
    """HTTP adapter for verified issue routes plus simulator-only record reads."""

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        *,
        timeout_seconds: float = 5,
        client: httpx.Client | None = None,
    ):
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = client or httpx.Client(
            base_url=base_url, headers=headers, timeout=timeout_seconds
        )

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
        params: dict[str, object] = {
            "region": target.region,
            "base_id": target.base_id,
            "page_number": page_number,
            "page_size": page_size,
        }
        params.update(
            {key: value for key, value in {
                "app_id": app_id, "table_id": table_id, "issue_id": issue_id
            }.items() if value is not None}
        )
        try:
            response = self._client.get(
                f"/v1/workspaces/{target.workspace_id}/issues", params=params
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise TransientAPIError(str(exc)) from exc
        self._raise_read_error(response)
        try:
            return ListIssuesResponse.model_validate(response.json())
        except (ValidationError, ValueError, KeyError) as exc:
            raise MalformedAPIResponse(f"invalid issue-list response: {exc}") from exc

    def retry_issue(self, target: SyncTarget, issue_id: str) -> ResolveIssuesResponse:
        try:
            response = self._client.post(
                f"/v1/workspaces/{target.workspace_id}/issues/action",
                params={"region": target.region},
                json={"ids": [issue_id], "action": "retry"},
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise AmbiguousOutcome(str(exc)) from exc
        if response.status_code >= 500:
            raise AmbiguousOutcome(f"remote status {response.status_code}")
        if response.status_code >= 400:
            raise ActionRejected(
                f"retry rejected with status {response.status_code}: {response.text}"
            )
        try:
            return ResolveIssuesResponse.model_validate(response.json())
        except (ValidationError, ValueError, KeyError) as exc:
            raise AmbiguousOutcome(f"invalid response after retry request: {exc}") from exc

    def read_record(
        self, side: str, app_id: str, table_id: str, record_id: str
    ) -> dict[str, Any]:
        if side not in {"source", "destination"}:
            raise ValueError(side)
        try:
            response = self._client.get(
                f"/simulator/apps/{app_id}/tables/{table_id}/{side}/{record_id}"
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise TransientAPIError(str(exc)) from exc
        if response.status_code == 404:
            raise MissingRecord(f"{side} record {record_id} was not found")
        self._raise_read_error(response)
        try:
            return dict(response.json()["record"])
        except (ValueError, KeyError, TypeError) as exc:
            raise MalformedAPIResponse(f"invalid record response: {exc}") from exc

    @staticmethod
    def _raise_read_error(response: httpx.Response) -> None:
        if response.status_code >= 500:
            raise TransientAPIError(f"remote status {response.status_code}")
        if response.status_code == 400:
            try:
                if response.json().get("error") == "Unexpected Error":
                    raise TransientAPIError("Stacksync Unexpected Error")
            except ValueError:
                pass
        response.raise_for_status()
