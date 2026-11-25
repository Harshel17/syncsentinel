from __future__ import annotations

import secrets
from collections.abc import Callable
from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from syncsentinel.contracts import WebhookEnvelope
from syncsentinel.domain import utc_now
from syncsentinel.gateway import StacksyncGateway
from syncsentinel.persistence import IncidentRepository
from syncsentinel.persistence import RecoveryApprovalConflict, StaleOccurrence


class RetryApproval(BaseModel):
    expected_issue_id: str = Field(min_length=1)
    approved_by: str = Field(min_length=1)
    correlation_id: str | None = None


def create_app(
    repository: IncidentRepository,
    *,
    webhook_bearer_secret: str,
    clock: Callable[[], datetime] = utc_now,
    gateway: StacksyncGateway | None = None,
    worker_tick: Callable[[], None] | None = None,
    scenario_controller: Callable[[str], None] | None = None,
    reset_controller: Callable[[], None] | None = None,
) -> FastAPI:
    app = FastAPI(title="SyncSentinel")

    @app.post("/webhooks/stacksync", status_code=202)
    def ingest_webhook(
        envelope: WebhookEnvelope,
        authorization: str | None = Header(default=None),
        x_correlation_id: str | None = Header(default=None),
    ) -> dict[str, object]:
        expected = f"Bearer {webhook_bearer_secret}"
        if authorization is None or not secrets.compare_digest(authorization, expected):
            raise HTTPException(status_code=403, detail="invalid webhook bearer")
        notification = envelope.notification
        target = repository.target_for_base(notification.base_id)
        if target is None:
            raise HTTPException(status_code=404, detail="unknown sync base")
        correlation_id = x_correlation_id or str(uuid4())
        job_id, created = repository.schedule_reconciliation(
            target,
            clock(),
            correlation_id,
            app_id=notification.app_id,
            table_id=notification.internal_table_id,
            reason="webhook_hint",
        )
        return {"reconciliation_job_id": job_id, "scheduled": created}

    @app.get("/incidents")
    def list_incidents() -> list[dict[str, object]]:
        audit = repository.audit_log()
        results = []
        for incident in repository.list_incidents():
            observation = repository.get_observation(incident.latest_issue_id)
            discovery = next(
                (
                    event
                    for event in audit
                    if event["incident_id"] == incident.id
                    and event["event_type"] == "issue_discovered"
                ),
                None,
            )
            correlation_id = discovery["correlation_id"] if discovery else ""
            webhook_correlations = {
                event["correlation_id"]
                for event in audit
                if event["event_type"] == "webhook_received"
            }
            results.append(
                {
                    "id": incident.id,
                    "latest_issue_id": incident.latest_issue_id,
                    "base_id": incident.base_id,
                    "destination_app_id": incident.destination_app_id,
                    "destination_table_id": incident.destination_table_id,
                    "record_identity": incident.record_identity,
                    "state": incident.state,
                    "version": incident.version,
                    "created_at": incident.created_at,
                    "updated_at": incident.updated_at,
                    "action": observation.action if observation else None,
                    "issue_message": observation.issue_message if observation else None,
                    "source_record_id": observation.source_record_id if observation else None,
                    "destination_record_id": (
                        observation.destination_record_id if observation else None
                    ),
                    "detection_method": (
                        "webhook hint"
                        if correlation_id in webhook_correlations
                        else "scheduled reconciliation"
                    ),
                }
            )
        return results

    @app.get("/overview")
    def overview() -> dict[str, int]:
        incidents = repository.list_incidents()
        return {
            "total": len(incidents),
            "open": sum(incident.state != "resolved" for incident in incidents),
            "awaiting_approval": sum(
                incident.state == "awaiting_approval" for incident in incidents
            ),
            "verified_recoveries": sum(
                incident.state == "resolved" for incident in incidents
            ),
            "manual_review": sum(
                incident.state == "manual_review" for incident in incidents
            ),
        }

    @app.get("/incidents/{incident_id}")
    def incident_detail(incident_id: str) -> dict[str, object]:
        incident = repository.get(incident_id)
        if incident is None:
            raise HTTPException(status_code=404, detail="incident not found")
        observation = repository.get_observation(incident.latest_issue_id)
        comparison: dict[str, object] = {
            "available": False,
            "fields": [],
            "warning": "Verification information is unavailable.",
        }
        issue_open: bool | None = None
        if gateway and observation:
            target = repository.target_for_base(incident.base_id)
            try:
                if (
                    target
                    and observation.source_record_id
                    and observation.destination_record_id
                    and observation.destination_table_id
                    and observation.changed_columns
                ):
                    source = gateway.read_record(
                        "source",
                        observation.destination_app_id,
                        observation.destination_table_id,
                        observation.source_record_id,
                    )
                    destination = gateway.read_record(
                        "destination",
                        observation.destination_app_id,
                        observation.destination_table_id,
                        observation.destination_record_id,
                    )
                    fields = [
                        {
                            "name": name,
                            "source": source.get(name),
                            "destination": destination.get(name),
                            "matches": source.get(name) == destination.get(name),
                        }
                        for name in sorted(observation.changed_columns)
                    ]
                    comparison = {"available": True, "fields": fields, "warning": None}
                if target:
                    open_page = gateway.list_issues(
                        target,
                        page_number=0,
                        page_size=1,
                        issue_id=incident.latest_issue_id,
                    )
                    issue_open = bool(open_page.data)
            except Exception as exc:
                comparison["warning"] = f"Verification read unavailable: {exc}"
        timeline = [
            event
            for event in repository.audit_log()
            if event["incident_id"] == incident.id
        ]
        verification = next(
            (
                event
                for event in reversed(timeline)
                if event["event_type"]
                in {"verification_succeeded", "verification_failed"}
            ),
            None,
        )
        return {
            "incident": next(
                item for item in list_incidents() if item["id"] == incident.id
            ),
            "occurrence": (
                {
                    "issue_id": observation.issue_id,
                    "action": observation.action,
                    "issue_message": observation.issue_message,
                    "changed_columns": observation.changed_columns,
                    "created_time": observation.created_time,
                    "last_modified_time": observation.last_modified_time,
                }
                if observation
                else None
            ),
            "comparison": comparison,
            "issue_open": issue_open,
            # The demo supports one occurrence-specific approval. Manual-review
            # incidents need investigation, not an unsupported repeated request.
            "recovery_eligible": incident.state == "discovered" and issue_open is True,
            "verification_evidence": verification,
            "timeline": timeline,
        }

    @app.post("/incidents/{incident_id}/approve-retry", status_code=202)
    def approve_retry(incident_id: str, approval: RetryApproval) -> dict[str, object]:
        try:
            job = repository.approve_retry(
                incident_id,
                approval.expected_issue_id,
                approval.approved_by,
                approval.correlation_id or str(uuid4()),
                clock(),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="incident not found") from exc
        except (RuntimeError, RecoveryApprovalConflict, StaleOccurrence) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"recovery_job_id": job.id, "issue_id": job.issue_id, "status": job.status}

    @app.get("/audit")
    def audit_log() -> list[dict[str, object]]:
        return repository.audit_log()

    @app.get("/health")
    def health() -> dict[str, object]:
        audit = repository.audit_log()
        last_worker = next(
            (
                event
                for event in reversed(audit)
                if event["event_type"] == "worker_restarted"
            ),
            None,
        )
        connected = False
        if gateway:
            target = next(
                (
                    repository.target_for_base(incident.base_id)
                    for incident in repository.list_incidents()
                ),
                None,
            )
            if target:
                try:
                    gateway.list_issues(target, page_number=0, page_size=1)
                    connected = True
                except Exception:
                    connected = False
        return {
            "backend": "online",
            "simulator_connected": connected,
            "worker": "ready" if worker_tick else "external",
            "last_worker_event": last_worker["occurred_at"] if last_worker else None,
        }

    @app.post("/demo/worker/tick")
    def run_demo_worker() -> dict[str, str]:
        if worker_tick is None:
            raise HTTPException(status_code=404, detail="demo worker unavailable")
        worker_tick()
        return {"status": "completed"}

    @app.post("/demo/scenarios/{scenario}")
    def load_scenario(scenario: str) -> dict[str, str]:
        if scenario_controller is None:
            raise HTTPException(status_code=404, detail="scenario controls unavailable")
        try:
            scenario_controller(scenario)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "ready", "scenario": scenario}

    @app.post("/demo/reset")
    def reset_demo() -> dict[str, str]:
        if reset_controller is None:
            raise HTTPException(status_code=404, detail="reset unavailable")
        reset_controller()
        return {"status": "reset"}

    return app
