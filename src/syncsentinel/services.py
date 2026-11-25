from __future__ import annotations

from datetime import datetime

from syncsentinel.gateway import (
    ActionRejected,
    AmbiguousOutcome,
    MalformedAPIResponse,
    MissingRecord,
    StacksyncGateway,
    TransientAPIError,
)
from syncsentinel.persistence import (
    IncidentRepository,
    LeaseConflict,
    RecoveryJob,
    StaleOccurrence,
    SyncTarget,
)


class ReconciliationEngine:
    def __init__(
        self,
        repository: IncidentRepository,
        gateway: StacksyncGateway,
        *,
        page_size: int = 100,
        max_attempts: int = 3,
    ):
        self.repository = repository
        self.gateway = gateway
        self.page_size = page_size
        self.max_attempts = max_attempts

    def run_once(self, worker_id: str, now: datetime) -> bool:
        job = self.repository.claim_reconciliation(worker_id, now)
        if job is None:
            return False
        target = SyncTarget(job.workspace_id, job.region, job.base_id)
        try:
            page_number = 0
            while True:
                page = self.gateway.list_issues(
                    target,
                    page_number=page_number,
                    page_size=self.page_size,
                    app_id=job.app_id,
                    table_id=job.table_id,
                )
                for issue in page.data:
                    self.repository.record_observation(
                        issue.to_observation(), now, job.correlation_id
                    )
                if page.pagination.next_page is None:
                    break
                page_number = page.pagination.next_page
            try:
                self.repository.finish_reconciliation(job, now)
            except LeaseConflict:
                pass
        except Exception as exc:
            try:
                self.repository.finish_reconciliation(
                    job, now, error=str(exc), max_attempts=self.max_attempts
                )
            except LeaseConflict:
                pass
        return True


class RecoveryEngine:
    def __init__(self, repository: IncidentRepository, gateway: StacksyncGateway):
        self.repository = repository
        self.gateway = gateway

    def run_recovery_once(self, worker_id: str, now: datetime) -> bool:
        job = self.repository.claim_recovery(worker_id, now)
        if job is None:
            return False
        incident = self.repository.get(job.incident_id)
        if incident is None:
            try:
                self.repository.reject_stale_recovery(job, now, "incident no longer exists")
            except LeaseConflict:
                pass
            return True
        target = self.repository.target_for_base(incident.base_id)
        if target is None:
            try:
                self.repository.reject_stale_recovery(job, now, "sync target is not configured")
            except LeaseConflict:
                pass
            return True
        try:
            page = self.gateway.list_issues(
                target, page_number=0, page_size=1, issue_id=job.issue_id
            )
        except TransientAPIError as exc:
            try:
                self.repository.reject_stale_recovery(
                    job, now, f"could not validate current occurrence: {exc}"
                )
            except LeaseConflict:
                pass
            return True
        if not page.data or page.data[0].issue_id != job.issue_id:
            try:
                self.repository.reject_stale_recovery(
                    job, now, "approved issue occurrence is no longer current"
                )
            except LeaseConflict:
                pass
            return True
        try:
            self.repository.begin_recovery_execution(job, now)
        except StaleOccurrence:
            try:
                self.repository.reject_stale_recovery(
                    job, now, "a newer issue occurrence replaced the approved occurrence"
                )
            except LeaseConflict:
                pass
            return True
        except LeaseConflict:
            return True
        try:
            result = self.gateway.retry_issue(target, job.issue_id)
            succeeded = result.data.success_issue_count == 1
            if succeeded:
                self.repository.recovery_needs_verification(
                    job,
                    now,
                    ambiguous=False,
                    reason="retry accepted; independent verification required",
                )
            else:
                self.repository.reject_recovery_execution(
                    job, now, "retry API explicitly reported failure"
                )
        except ActionRejected as exc:
            try:
                self.repository.reject_recovery_execution(
                    job, now, f"retry API rejected request: {exc}"
                )
            except LeaseConflict:
                pass
        except AmbiguousOutcome as exc:
            try:
                self.repository.recovery_needs_verification(
                    job,
                    now,
                    ambiguous=True,
                    reason=f"remote outcome unknown: {exc}",
                )
            except LeaseConflict:
                pass
        except LeaseConflict:
            pass
        return True

    def run_verification_once(self, worker_id: str, now: datetime) -> bool:
        job = self.repository.claim_verification(worker_id, now)
        if job is None:
            return False
        observation = self.repository.get_observation(job.issue_id)
        if observation is None:
            self.repository.finish_verification(
                job, now, converged=False, reason="issue observation is unavailable"
            )
            return True
        if not observation.changed_columns:
            self.repository.finish_verification(
                job,
                now,
                converged=False,
                reason="issue has no changed_columns to verify safely",
            )
            return True
        source_id = observation.source_record_id
        destination_id = observation.destination_record_id
        if not source_id or not destination_id or not observation.destination_table_id:
            self.repository.finish_verification(
                job,
                now,
                converged=False,
                reason="both source and destination record ids are required",
            )
            return True
        try:
            source_before = self.gateway.read_record(
                "source",
                observation.destination_app_id,
                observation.destination_table_id,
                source_id,
            )
            destination = self.gateway.read_record(
                "destination",
                observation.destination_app_id,
                observation.destination_table_id,
                destination_id,
            )
            source_after = self.gateway.read_record(
                "source",
                observation.destination_app_id,
                observation.destination_table_id,
                source_id,
            )
        except (MissingRecord, KeyError) as exc:
            self.repository.finish_verification(
                job, now, converged=False, reason=f"verification read failed: {exc}"
            )
            return True
        except (TransientAPIError, MalformedAPIResponse) as exc:
            try:
                self.repository.defer_verification(
                    job, now, f"verification read failed: {exc}"
                )
            except LeaseConflict:
                pass
            return True
        except LeaseConflict:
            return True
        fields = tuple(sorted(observation.changed_columns))
        if source_before != source_after:
            converged = False
            reason = "source changed during verification"
        else:
            mismatches = [
                field
                for field in fields
                if source_after.get(field) != destination.get(field)
            ]
            converged = not mismatches
            reason = (
                f"verified convergence for fields: {', '.join(fields)}"
                if converged
                else f"record mismatch for fields: {', '.join(mismatches)}"
            )
        incident = self.repository.get(job.incident_id)
        target = self.repository.target_for_base(incident.base_id) if incident else None
        if converged and target is not None:
            try:
                open_page = self.gateway.list_issues(
                    target, page_number=0, page_size=1, issue_id=job.issue_id
                )
                if open_page.data:
                    converged = False
                    reason = "records converge but the issue occurrence remains open"
            except (TransientAPIError, MalformedAPIResponse) as exc:
                try:
                    self.repository.defer_verification(
                        job, now, f"could not confirm issue closure: {exc}"
                    )
                except LeaseConflict:
                    pass
                return True
        elif target is None:
            converged = False
            reason = "sync target is unavailable for issue-closure verification"
        try:
            self.repository.finish_verification(
                job, now, converged=converged, reason=reason
            )
        except LeaseConflict:
            pass
        return True
