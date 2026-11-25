from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator
from uuid import NAMESPACE_URL, uuid4, uuid5

from syncsentinel.domain import Incident, IncidentState, IssueObservation, RecoveryAction


SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS incidents (
 id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE, latest_issue_id TEXT NOT NULL,
 base_id TEXT NOT NULL, destination_app_id TEXT NOT NULL,
 destination_table_id TEXT NOT NULL, record_identity TEXT NOT NULL, state TEXT NOT NULL,
 recovery_action TEXT, version INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS issue_observations (
 issue_id TEXT PRIMARY KEY, incident_id TEXT NOT NULL REFERENCES incidents(id),
 observed_at TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS incident_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, incident_id TEXT NOT NULL REFERENCES incidents(id),
 event_type TEXT NOT NULL, from_state TEXT, to_state TEXT, reason TEXT NOT NULL, occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL,
 correlation_id TEXT NOT NULL, incident_id TEXT, job_id TEXT, from_state TEXT,
 to_state TEXT, details TEXT NOT NULL, occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_targets (
 base_id TEXT PRIMARY KEY, workspace_id INTEGER NOT NULL, region TEXT NOT NULL,
 poll_interval_seconds INTEGER NOT NULL, next_poll_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reconciliation_jobs (
 id TEXT PRIMARY KEY, coalesce_key TEXT NOT NULL UNIQUE, workspace_id INTEGER NOT NULL,
 region TEXT NOT NULL, base_id TEXT NOT NULL, app_id TEXT, table_id TEXT,
 status TEXT NOT NULL, available_at TEXT NOT NULL, leased_until TEXT, lease_owner TEXT,
 attempt_count INTEGER NOT NULL, correlation_id TEXT NOT NULL, last_error TEXT,
 lease_token TEXT, rerun_requested INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_jobs (
 id TEXT PRIMARY KEY, incident_id TEXT NOT NULL REFERENCES incidents(id), issue_id TEXT NOT NULL,
 action TEXT NOT NULL, status TEXT NOT NULL, approved_by TEXT NOT NULL,
 correlation_id TEXT NOT NULL, available_at TEXT NOT NULL, leased_until TEXT,
 lease_owner TEXT, lease_token TEXT, attempt_count INTEGER NOT NULL,
 verification_attempt_count INTEGER NOT NULL DEFAULT 0,
 ambiguous INTEGER NOT NULL DEFAULT 0,
 last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(incident_id, issue_id, action)
);
CREATE INDEX IF NOT EXISTS idx_events_incident ON incident_events(incident_id, sequence);
CREATE INDEX IF NOT EXISTS idx_reconciliation_claim ON reconciliation_jobs(status, available_at);
CREATE INDEX IF NOT EXISTS idx_recovery_claim ON recovery_jobs(status, available_at);
"""


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


@dataclass(frozen=True, slots=True)
class SyncTarget:
    workspace_id: int
    region: str
    base_id: str
    poll_interval_seconds: int = 300


@dataclass(frozen=True, slots=True)
class ReconciliationJob:
    id: str
    workspace_id: int
    region: str
    base_id: str
    app_id: str | None
    table_id: str | None
    attempt_count: int
    correlation_id: str
    lease_owner: str | None
    lease_token: str | None


@dataclass(frozen=True, slots=True)
class RecoveryJob:
    id: str
    incident_id: str
    issue_id: str
    action: RecoveryAction
    status: str
    attempt_count: int
    correlation_id: str
    lease_owner: str | None
    lease_token: str | None
    ambiguous: bool
    verification_attempt_count: int


class StaleOccurrence(RuntimeError):
    pass


class LeaseConflict(RuntimeError):
    pass


class RecoveryApprovalConflict(RuntimeError):
    pass


class IncidentRepository:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(SCHEMA)
            self._ensure_column(connection, "reconciliation_jobs", "lease_token TEXT")
            self._ensure_column(
                connection,
                "reconciliation_jobs",
                "rerun_requested INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(connection, "recovery_jobs", "lease_token TEXT")
            self._ensure_column(
                connection,
                "recovery_jobs",
                "verification_attempt_count INTEGER NOT NULL DEFAULT 0",
            )

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection, table: str, definition: str
    ) -> None:
        name = definition.split()[0]
        existing = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if name not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
        finally:
            connection.close()

    def record_observation(
        self,
        issue: IssueObservation,
        observed_at: datetime,
        correlation_id: str = "reconciliation",
    ) -> tuple[Incident, bool]:
        candidate = Incident.from_observation(issue, observed_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                duplicate = connection.execute(
                    "SELECT incident_id FROM issue_observations WHERE issue_id=?",
                    (issue.issue_id,),
                ).fetchone()
                if duplicate:
                    incident = self._get_incident(connection, duplicate["incident_id"])
                    if incident.state is IncidentState.RESOLVED:
                        previous, expected = incident.state, incident.version
                        incident.transition(IncidentState.DISCOVERED, observed_at)
                        self._save_transition(
                            connection,
                            incident,
                            previous,
                            "polling observed the same issue still open",
                            observed_at,
                            expected,
                        )
                        self._append_audit(
                            connection,
                            "incident_reopened",
                            correlation_id,
                            observed_at,
                            incident_id=incident.id,
                            from_state=previous,
                            to_state=IncidentState.DISCOVERED,
                            details={"issue_id": issue.issue_id},
                        )
                    self._append_audit(
                        connection, "duplicate_suppressed", correlation_id, observed_at,
                        incident_id=incident.id, details={"issue_id": issue.issue_id},
                    )
                    connection.commit()
                    return incident, False

                row = connection.execute(
                    "SELECT * FROM incidents WHERE fingerprint=?", (issue.fingerprint,)
                ).fetchone()
                stale = False
                if row is None:
                    incident = candidate
                    connection.execute(
                        """INSERT INTO incidents
                        (id,fingerprint,latest_issue_id,base_id,destination_app_id,
                         destination_table_id,record_identity,state,recovery_action,version,
                         created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        self._incident_values(incident),
                    )
                    self._append_event(
                        connection, incident.id, "incident_discovered", None,
                        IncidentState.DISCOVERED, "new issue observation", observed_at,
                    )
                else:
                    incident = self._incident_from_row(row)
                    previous_payload = self._observation_payload(
                        connection, incident.latest_issue_id
                    )
                    stale = bool(
                        previous_payload
                        and issue.last_modified_time < previous_payload["last_modified_time"]
                    )
                    if not stale:
                        previous = incident.state
                        if previous is IncidentState.RESOLVED:
                            incident.transition(IncidentState.DISCOVERED, observed_at)
                            event_type = "incident_recurred"
                        else:
                            incident.version += 1
                            incident.updated_at = observed_at
                            event_type = "issue_refreshed"
                        incident.latest_issue_id = issue.issue_id
                        connection.execute(
                            """UPDATE incidents SET latest_issue_id=?,state=?,version=?,updated_at=?
                               WHERE id=?""",
                            (incident.latest_issue_id, incident.state, incident.version,
                             _iso(incident.updated_at), incident.id),
                        )
                        self._append_event(
                            connection, incident.id, event_type, previous, incident.state,
                            "new failure occurrence for stable record identity", observed_at,
                        )

                connection.execute(
                    "INSERT INTO issue_observations VALUES (?,?,?,?)",
                    (issue.issue_id, incident.id, _iso(observed_at), self._issue_json(issue)),
                )
                self._append_audit(
                    connection,
                    "stale_occurrence_suppressed" if stale else "issue_discovered",
                    correlation_id,
                    observed_at,
                    incident_id=incident.id,
                    details={"issue_id": issue.issue_id, "stale": stale},
                )
                connection.commit()
                return incident, True
            except Exception:
                connection.rollback()
                raise

    def transition(
        self,
        incident_id: str,
        target: IncidentState,
        reason: str,
        occurred_at: datetime,
        expected_version: int,
        correlation_id: str = "state-transition",
    ) -> Incident:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                incident = self._get_incident(connection, incident_id)
                if incident.version != expected_version:
                    raise RuntimeError("incident was modified concurrently")
                previous = incident.state
                incident.transition(target, occurred_at)
                self._save_transition(
                    connection, incident, previous, reason, occurred_at, expected_version
                )
                self._append_audit(
                    connection, "state_transition", correlation_id, occurred_at,
                    incident_id=incident.id, from_state=previous, to_state=target,
                    details={"reason": reason},
                )
                connection.commit()
                return incident
            except Exception:
                connection.rollback()
                raise

    def get(self, incident_id: str) -> Incident | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM incidents WHERE id=?", (incident_id,)
            ).fetchone()
            return None if row is None else self._incident_from_row(row)

    def get_observation(self, issue_id: str) -> IssueObservation | None:
        with self._connect() as connection:
            payload = self._observation_payload(connection, issue_id)
            return IssueObservation(**payload) if payload else None

    def history(self, incident_id: str) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT sequence,event_type,from_state,to_state,reason,occurred_at
                   FROM incident_events WHERE incident_id=? ORDER BY sequence""",
                (incident_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_incidents(self) -> list[Incident]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM incidents ORDER BY created_at,id"
            ).fetchall()
            return [self._incident_from_row(row) for row in rows]

    def reset_demo_data(self, now: datetime) -> None:
        """Clear only local demonstration state; configured sync targets remain."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("DELETE FROM recovery_jobs")
                connection.execute("DELETE FROM reconciliation_jobs")
                connection.execute("DELETE FROM audit_events")
                connection.execute("DELETE FROM incident_events")
                connection.execute("DELETE FROM issue_observations")
                connection.execute("DELETE FROM incidents")
                connection.execute(
                    "UPDATE sync_targets SET next_poll_at=?", (_iso(now),)
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def register_target(self, target: SyncTarget, now: datetime) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO sync_targets
                   (base_id,workspace_id,region,poll_interval_seconds,next_poll_at)
                   VALUES (?,?,?,?,?) ON CONFLICT(base_id) DO UPDATE SET
                   workspace_id=excluded.workspace_id,region=excluded.region,
                   poll_interval_seconds=excluded.poll_interval_seconds""",
                (target.base_id, target.workspace_id, target.region,
                 target.poll_interval_seconds, _iso(now)),
            )

    def target_for_base(self, base_id: str) -> SyncTarget | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sync_targets WHERE base_id=?", (base_id,)
            ).fetchone()
            return None if row is None else SyncTarget(
                row["workspace_id"], row["region"], row["base_id"],
                row["poll_interval_seconds"],
            )

    def schedule_due_targets(self, now: datetime) -> int:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM sync_targets WHERE next_poll_at<=? ORDER BY base_id",
                (_iso(now),),
            ).fetchall()
        for row in rows:
            target = SyncTarget(row["workspace_id"], row["region"], row["base_id"],
                                row["poll_interval_seconds"])
            self.schedule_reconciliation(
                target, now, f"scheduled:{target.base_id}:{_iso(now)}",
                reason="scheduled_poll",
            )
            with self._connect() as connection:
                connection.execute(
                    "UPDATE sync_targets SET next_poll_at=? WHERE base_id=?",
                    (_iso(now + timedelta(seconds=target.poll_interval_seconds)), target.base_id),
                )
        return len(rows)

    def schedule_reconciliation(
        self,
        target: SyncTarget,
        now: datetime,
        correlation_id: str,
        *,
        app_id: str | None = None,
        table_id: str | None = None,
        reason: str = "webhook_hint",
    ) -> tuple[str, bool]:
        # Base-wide coalescing means any hint causes the correctness-preserving full scan.
        key = f"{target.workspace_id}:{target.region}:{target.base_id}"
        job_id = str(uuid5(NAMESPACE_URL, f"syncsentinel:reconcile:{key}"))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT status FROM reconciliation_jobs WHERE id=?", (job_id,)
                ).fetchone()
                created = (
                    row is None
                    or row["status"] in {"completed", "failed"}
                    or row["status"] == "leased"
                )
                if row is None:
                    connection.execute(
                        """INSERT INTO reconciliation_jobs
                        (id,coalesce_key,workspace_id,region,base_id,app_id,table_id,
                         status,available_at,leased_until,lease_owner,attempt_count,
                         correlation_id,last_error,lease_token,rerun_requested,created_at,updated_at)
                        VALUES (?,?,?,?,?,?,?,'pending',?,NULL,NULL,0,?,NULL,NULL,0,?,?)""",
                        (job_id, key, target.workspace_id, target.region, target.base_id,
                         None, None, _iso(now), correlation_id, _iso(now), _iso(now)),
                    )
                elif row["status"] in {"completed", "failed"}:
                    connection.execute(
                        """UPDATE reconciliation_jobs SET status='pending',available_at=?,
                           leased_until=NULL,lease_owner=NULL,attempt_count=0,correlation_id=?,
                           lease_token=NULL,rerun_requested=0,last_error=NULL,updated_at=?
                           WHERE id=?""",
                        (_iso(now), correlation_id, _iso(now), job_id),
                    )
                elif row["status"] == "leased":
                    connection.execute(
                        """UPDATE reconciliation_jobs SET rerun_requested=1,updated_at=?
                           WHERE id=? AND status='leased'""",
                        (_iso(now), job_id),
                    )
                self._append_audit(
                    connection,
                    "reconciliation_scheduled" if created else "notification_coalesced",
                    correlation_id, now, job_id=job_id,
                    details={"reason": reason, "base_id": target.base_id,
                             "app_id": app_id, "table_id": table_id},
                )
                if reason == "webhook_hint":
                    self._append_audit(
                        connection, "webhook_received", correlation_id, now, job_id=job_id,
                        details={"base_id": target.base_id},
                    )
                connection.commit()
                return job_id, created
            except Exception:
                connection.rollback()
                raise

    def claim_reconciliation(
        self, worker_id: str, now: datetime, lease_seconds: int = 30
    ) -> ReconciliationJob | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM reconciliation_jobs WHERE status='pending'
                       AND available_at<=? ORDER BY available_at,id LIMIT 1""",
                    (_iso(now),),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                lease_token = str(uuid4())
                connection.execute(
                    """UPDATE reconciliation_jobs SET status='leased',lease_owner=?,
                       lease_token=?,leased_until=?,attempt_count=attempt_count+1,
                       updated_at=? WHERE id=?""",
                    (worker_id, lease_token, _iso(now + timedelta(seconds=lease_seconds)),
                     _iso(now), row["id"]),
                )
                connection.commit()
                return ReconciliationJob(
                    row["id"], row["workspace_id"], row["region"], row["base_id"],
                    row["app_id"], row["table_id"], row["attempt_count"] + 1,
                    row["correlation_id"], worker_id, lease_token,
                )
            except Exception:
                connection.rollback()
                raise

    def finish_reconciliation(
        self,
        job: ReconciliationJob,
        now: datetime,
        *,
        error: str | None = None,
        max_attempts: int = 3,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    """SELECT rerun_requested FROM reconciliation_jobs
                       WHERE id=? AND status='leased' AND lease_token=?""",
                    (job.id, job.lease_token),
                ).fetchone()
                if current is None:
                    raise LeaseConflict("reconciliation lease is no longer owned")
                if error is None and current["rerun_requested"]:
                    status, available, event = "pending", now, "reconciliation_rerun"
                elif error is None:
                    status, available, event = "completed", now, "reconciliation_completed"
                elif job.attempt_count < max_attempts:
                    status = "pending"
                    available = now + timedelta(
                        seconds=min(2 ** (job.attempt_count - 1), 30)
                    )
                    event = "reconciliation_backoff"
                else:
                    status, available, event = "failed", now, "reconciliation_failed"
                changed = connection.execute(
                    """UPDATE reconciliation_jobs SET status=?,available_at=?,leased_until=NULL,
                       lease_owner=NULL,lease_token=NULL,rerun_requested=0,last_error=?,updated_at=?
                       WHERE id=? AND status='leased' AND lease_token=?""",
                    (status, _iso(available), error, _iso(now), job.id, job.lease_token),
                ).rowcount
                if changed != 1:
                    raise LeaseConflict("reconciliation lease is no longer owned")
                self._append_audit(
                    connection, event, job.correlation_id, now, job_id=job.id,
                    details={"error": error, "attempt": job.attempt_count},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def approve_retry(
        self,
        incident_id: str,
        expected_issue_id: str,
        approved_by: str,
        correlation_id: str,
        now: datetime,
    ) -> RecoveryJob:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                incident = self._get_incident(connection, incident_id)
                issue_id = incident.latest_issue_id
                if issue_id != expected_issue_id:
                    raise StaleOccurrence(
                        f"expected {expected_issue_id}, current occurrence is {issue_id}"
                    )
                job_id = str(uuid5(
                    NAMESPACE_URL, f"syncsentinel:recovery:{incident.id}:{issue_id}:retry"
                ))
                existing = connection.execute(
                    "SELECT * FROM recovery_jobs WHERE id=?", (job_id,)
                ).fetchone()
                if existing:
                    raise RecoveryApprovalConflict(
                        "a recovery approval already exists for this occurrence"
                    )
                if incident.state in {IncidentState.DISCOVERED, IncidentState.MANUAL_REVIEW}:
                    previous, expected = incident.state, incident.version
                    incident.transition(IncidentState.AWAITING_APPROVAL, now)
                    self._save_transition(connection, incident, previous,
                                          "operator review required", now, expected)
                if incident.state is not IncidentState.AWAITING_APPROVAL:
                    raise RuntimeError(f"incident cannot be approved from {incident.state}")
                previous, expected = incident.state, incident.version
                incident.transition(IncidentState.APPROVED, now)
                incident.recovery_action = RecoveryAction.RETRY
                self._save_transition(
                    connection, incident, previous, f"retry approved by {approved_by}",
                    now, expected, RecoveryAction.RETRY,
                )
                connection.execute(
                    """INSERT INTO recovery_jobs
                       (id,incident_id,issue_id,action,status,approved_by,correlation_id,
                        available_at,leased_until,lease_owner,attempt_count,ambiguous,
                        last_error,created_at,updated_at)
                       VALUES (?,?,?,'retry','pending',?,?,?,NULL,NULL,0,0,NULL,?,?)""",
                    (job_id, incident.id, issue_id, approved_by, correlation_id,
                     _iso(now), _iso(now), _iso(now)),
                )
                self._append_audit(
                    connection, "recovery_approved", correlation_id, now,
                    incident_id=incident.id, job_id=job_id, from_state=previous,
                    to_state=IncidentState.APPROVED,
                    details={"issue_id": issue_id, "action": "retry",
                             "approved_by": approved_by},
                )
                connection.commit()
                return RecoveryJob(
                    job_id, incident.id, issue_id, RecoveryAction.RETRY,
                    "pending", 0, correlation_id, None, None, False, 0,
                )
            except Exception:
                connection.rollback()
                raise

    def claim_recovery(
        self, worker_id: str, now: datetime, lease_seconds: int = 30
    ) -> RecoveryJob | None:
        return self._claim_recovery(("pending",), "leased", worker_id, now, lease_seconds)

    def begin_recovery_execution(self, job: RecoveryJob, now: datetime) -> Incident:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM recovery_jobs WHERE id=?", (job.id,)
                ).fetchone()
                if (
                    not row
                    or row["status"] != "leased"
                    or row["lease_token"] != job.lease_token
                ):
                    raise LeaseConflict("recovery lease is no longer owned")
                incident = self._get_incident(connection, job.incident_id)
                if incident.latest_issue_id != job.issue_id:
                    raise StaleOccurrence(job.issue_id)
                previous, expected = incident.state, incident.version
                incident.transition(IncidentState.EXECUTING, now)
                self._save_transition(connection, incident, previous,
                                      "calling Stacksync retry", now, expected)
                self._append_audit(
                    connection, "recovery_execution_started", job.correlation_id, now,
                    incident_id=incident.id, job_id=job.id, from_state=previous,
                    to_state=IncidentState.EXECUTING, details={"issue_id": job.issue_id},
                )
                connection.commit()
                return incident
            except Exception:
                connection.rollback()
                raise

    def recovery_needs_verification(
        self, job: RecoveryJob, now: datetime, *, ambiguous: bool, reason: str
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                owned = connection.execute(
                    """SELECT 1 FROM recovery_jobs
                       WHERE id=? AND status='leased' AND lease_token=?""",
                    (job.id, job.lease_token),
                ).fetchone()
                if owned is None:
                    raise LeaseConflict("recovery lease is no longer owned")
                incident = self._get_incident(connection, job.incident_id)
                previous = incident.state
                if incident.state is IncidentState.EXECUTING:
                    expected = incident.version
                    incident.transition(IncidentState.VERIFYING, now)
                    self._save_transition(connection, incident, previous, reason, now, expected)
                changed = connection.execute(
                    """UPDATE recovery_jobs SET status='verify_pending',ambiguous=?,
                       leased_until=NULL,lease_owner=NULL,last_error=?,updated_at=?
                       WHERE id=? AND status='leased' AND lease_token=?""",
                    (int(ambiguous), reason, _iso(now), job.id, job.lease_token),
                ).rowcount
                if changed != 1:
                    raise LeaseConflict("recovery lease is no longer owned")
                self._append_audit(
                    connection,
                    "ambiguous_api_outcome" if ambiguous else "recovery_api_succeeded",
                    job.correlation_id, now, incident_id=job.incident_id, job_id=job.id,
                    from_state=previous, to_state=IncidentState.VERIFYING,
                    details={"reason": reason},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def reject_stale_recovery(self, job: RecoveryJob, now: datetime, reason: str) -> None:
        self._fail_recovery(job, now, "stale_recovery_rejected", reason)

    def reject_recovery_execution(
        self, job: RecoveryJob, now: datetime, reason: str
    ) -> None:
        self._fail_recovery(job, now, "recovery_api_rejected", reason)

    def claim_verification(
        self, worker_id: str, now: datetime, lease_seconds: int = 30
    ) -> RecoveryJob | None:
        return self._claim_recovery(
            ("verify_pending",), "verifying_leased", worker_id, now, lease_seconds
        )

    def defer_verification(
        self, job: RecoveryJob, now: datetime, reason: str, max_attempts: int = 3
    ) -> None:
        if job.verification_attempt_count >= max_attempts:
            self.finish_verification(job, now, converged=False, reason=reason)
            return
        delay = min(2 ** (job.verification_attempt_count - 1), 30)
        with self._connect() as connection:
            changed = connection.execute(
                """UPDATE recovery_jobs SET status='verify_pending',available_at=?,
                   lease_owner=NULL,lease_token=NULL,leased_until=NULL,last_error=?,updated_at=?
                   WHERE id=? AND status='verifying_leased' AND lease_token=?""",
                (
                    _iso(now + timedelta(seconds=delay)), reason, _iso(now),
                    job.id, job.lease_token,
                ),
            ).rowcount
            if changed != 1:
                raise LeaseConflict("verification lease is no longer owned")

    def finish_verification(
        self, job: RecoveryJob, now: datetime, *, converged: bool, reason: str
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                owned = connection.execute(
                    """SELECT 1 FROM recovery_jobs WHERE id=?
                       AND status='verifying_leased' AND lease_token=?""",
                    (job.id, job.lease_token),
                ).fetchone()
                if owned is None:
                    raise LeaseConflict("verification lease is no longer owned")
                incident = self._get_incident(connection, job.incident_id)
                current = incident.latest_issue_id == job.issue_id
                success = converged and current
                previous = incident.state
                target = IncidentState.RESOLVED if success else IncidentState.MANUAL_REVIEW
                if incident.state is IncidentState.VERIFYING:
                    expected = incident.version
                    incident.transition(target, now)
                    self._save_transition(connection, incident, previous, reason, now, expected)
                changed = connection.execute(
                    """UPDATE recovery_jobs SET status=?,leased_until=NULL,lease_owner=NULL,
                       last_error=?,updated_at=? WHERE id=? AND status='verifying_leased'
                       AND lease_token=?""",
                    ("completed" if success else "unresolved", None if success else reason,
                     _iso(now), job.id, job.lease_token),
                ).rowcount
                if changed != 1:
                    raise LeaseConflict("verification lease is no longer owned")
                self._append_audit(
                    connection, "verification_succeeded" if success else "verification_failed",
                    job.correlation_id, now, incident_id=job.incident_id, job_id=job.id,
                    from_state=previous, to_state=target,
                    details={"reason": reason, "occurrence_current": current},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def recover_expired_leases(self, now: datetime, correlation_id: str) -> int:
        recovered = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = connection.execute(
                    """SELECT id FROM reconciliation_jobs WHERE status='leased'
                       AND leased_until<=?""", (_iso(now),),
                ).fetchall()
                for row in rows:
                    connection.execute(
                        """UPDATE reconciliation_jobs SET status='pending',lease_owner=NULL,
                           lease_token=NULL,leased_until=NULL,available_at=?,updated_at=?
                           WHERE id=?""",
                        (_iso(now), _iso(now), row["id"]),
                    )
                    self._append_audit(
                        connection, "lease_recovered", correlation_id, now, job_id=row["id"],
                        details={"job_type": "reconciliation"},
                    )
                    recovered += 1
                rows = connection.execute(
                    """SELECT * FROM recovery_jobs WHERE status IN ('leased','verifying_leased')
                       AND leased_until<=?""", (_iso(now),),
                ).fetchall()
                for row in rows:
                    execution = row["status"] == "leased"
                    connection.execute(
                        """UPDATE recovery_jobs SET status='verify_pending',ambiguous=?,
                           lease_owner=NULL,lease_token=NULL,leased_until=NULL,
                           available_at=?,last_error=?,updated_at=?
                           WHERE id=?""",
                        (int(bool(row["ambiguous"]) or execution), _iso(now),
                         "worker lease expired; external outcome unknown" if execution
                         else "verification worker lease expired", _iso(now), row["id"]),
                    )
                    incident = self._get_incident(connection, row["incident_id"])
                    if incident.state is IncidentState.APPROVED:
                        previous, expected = incident.state, incident.version
                        incident.transition(IncidentState.EXECUTING, now)
                        self._save_transition(connection, incident, previous,
                                              "expired execution lease", now, expected)
                    if incident.state is IncidentState.EXECUTING:
                        previous, expected = incident.state, incident.version
                        incident.transition(IncidentState.VERIFYING, now)
                        self._save_transition(
                            connection, incident, previous,
                            "external outcome unknown after worker restart", now, expected,
                        )
                    self._append_audit(
                        connection, "lease_recovered", correlation_id, now,
                        incident_id=row["incident_id"], job_id=row["id"],
                        details={"job_type": "recovery", "outcome_ambiguous": execution},
                    )
                    recovered += 1
                connection.commit()
                return recovered
            except Exception:
                connection.rollback()
                raise

    def audit_log(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events ORDER BY sequence"
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["details"] = json.loads(item["details"])
                result.append(item)
            return result

    def record_worker_started(
        self, worker_id: str, now: datetime, recovered_leases: int
    ) -> None:
        with self._connect() as connection:
            self._append_audit(
                connection,
                "worker_restarted",
                f"worker-start:{worker_id}",
                now,
                details={"worker_id": worker_id, "leases_recovered": recovered_leases},
            )

    def recovery_job(self, job_id: str) -> RecoveryJob | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_jobs WHERE id=?", (job_id,)
            ).fetchone()
            return None if row is None else self._recovery_from_row(row)

    def _claim_recovery(
        self, statuses: tuple[str, ...], claimed_status: str, worker_id: str,
        now: datetime, lease_seconds: int,
    ) -> RecoveryJob | None:
        placeholders = ",".join("?" for _ in statuses)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    f"""SELECT * FROM recovery_jobs WHERE status IN ({placeholders})
                        AND available_at<=? ORDER BY available_at,id LIMIT 1""",
                    (*statuses, _iso(now)),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                lease_token = str(uuid4())
                verification_increment = 1 if claimed_status == "verifying_leased" else 0
                connection.execute(
                    """UPDATE recovery_jobs SET status=?,lease_owner=?,lease_token=?,leased_until=?,
                       attempt_count=attempt_count+1,
                       verification_attempt_count=verification_attempt_count+?,updated_at=?
                       WHERE id=?""",
                    (claimed_status, worker_id, lease_token,
                     _iso(now + timedelta(seconds=lease_seconds)),
                     verification_increment, _iso(now), row["id"]),
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=claimed_status,
                    lease_owner=worker_id,
                    lease_token=lease_token,
                    attempt_count=row["attempt_count"] + 1,
                    verification_attempt_count=(
                        row["verification_attempt_count"] + verification_increment
                    ),
                )
                return self._recovery_from_mapping(values)
            except Exception:
                connection.rollback()
                raise

    def _fail_recovery(
        self, job: RecoveryJob, now: datetime, event_type: str, reason: str
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                owned = connection.execute(
                    """SELECT 1 FROM recovery_jobs
                       WHERE id=? AND status='leased' AND lease_token=?""",
                    (job.id, job.lease_token),
                ).fetchone()
                if owned is None:
                    raise LeaseConflict("recovery lease is no longer owned")
                incident = self._get_incident(connection, job.incident_id)
                previous = incident.state
                if incident.state in {IncidentState.APPROVED, IncidentState.EXECUTING}:
                    expected = incident.version
                    incident.transition(IncidentState.MANUAL_REVIEW, now)
                    self._save_transition(connection, incident, previous, reason, now, expected)
                changed = connection.execute(
                    """UPDATE recovery_jobs SET status='unresolved',lease_owner=NULL,
                       lease_token=NULL,leased_until=NULL,last_error=?,updated_at=?
                       WHERE id=? AND status='leased' AND lease_token=?""",
                    (reason, _iso(now), job.id, job.lease_token),
                ).rowcount
                if changed != 1:
                    raise LeaseConflict("recovery lease is no longer owned")
                self._append_audit(
                    connection, event_type, job.correlation_id, now,
                    incident_id=job.incident_id, job_id=job.id,
                    details={"reason": reason, "issue_id": job.issue_id},
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _issue_json(issue: IssueObservation) -> str:
        return json.dumps(
            {name: getattr(issue, name) for name in issue.__dataclass_fields__},
            sort_keys=True, separators=(",", ":"),
        )

    @staticmethod
    def _observation_payload(
        connection: sqlite3.Connection, issue_id: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT payload FROM issue_observations WHERE issue_id=?", (issue_id,)
        ).fetchone()
        return None if row is None else json.loads(row["payload"])

    @staticmethod
    def _get_incident(connection: sqlite3.Connection, incident_id: str) -> Incident:
        row = connection.execute(
            "SELECT * FROM incidents WHERE id=?", (incident_id,)
        ).fetchone()
        if row is None:
            raise KeyError(incident_id)
        return IncidentRepository._incident_from_row(row)

    @staticmethod
    def _incident_values(incident: Incident) -> tuple[object, ...]:
        return (
            incident.id, incident.fingerprint, incident.latest_issue_id,
            incident.base_id, incident.destination_app_id, incident.destination_table_id,
            incident.record_identity, incident.state, incident.recovery_action,
            incident.version, _iso(incident.created_at), _iso(incident.updated_at),
        )

    @staticmethod
    def _incident_from_row(row: sqlite3.Row) -> Incident:
        return Incident(
            id=row["id"], fingerprint=row["fingerprint"],
            latest_issue_id=row["latest_issue_id"], base_id=row["base_id"],
            destination_app_id=row["destination_app_id"],
            destination_table_id=row["destination_table_id"],
            record_identity=row["record_identity"], state=IncidentState(row["state"]),
            recovery_action=RecoveryAction(row["recovery_action"])
            if row["recovery_action"] else None,
            version=row["version"], created_at=_dt(row["created_at"]),
            updated_at=_dt(row["updated_at"]),
        )

    @staticmethod
    def _save_transition(
        connection: sqlite3.Connection, incident: Incident, previous: IncidentState,
        reason: str, occurred_at: datetime, expected_version: int,
        recovery_action: RecoveryAction | None = None,
    ) -> None:
        changed = connection.execute(
            """UPDATE incidents SET state=?,recovery_action=COALESCE(?,recovery_action),
               version=?,updated_at=? WHERE id=? AND version=?""",
            (incident.state, recovery_action, incident.version, _iso(incident.updated_at),
             incident.id, expected_version),
        ).rowcount
        if changed != 1:
            raise RuntimeError("incident was modified concurrently")
        IncidentRepository._append_event(
            connection, incident.id, "state_transition", previous, incident.state,
            reason, occurred_at,
        )

    @staticmethod
    def _append_event(
        connection: sqlite3.Connection, incident_id: str, event_type: str,
        from_state: IncidentState | None, to_state: IncidentState,
        reason: str, occurred_at: datetime,
    ) -> None:
        connection.execute(
            """INSERT INTO incident_events
               (incident_id,event_type,from_state,to_state,reason,occurred_at)
               VALUES (?,?,?,?,?,?)""",
            (incident_id, event_type, from_state, to_state, reason, _iso(occurred_at)),
        )

    @staticmethod
    def _append_audit(
        connection: sqlite3.Connection, event_type: str, correlation_id: str,
        occurred_at: datetime, *, incident_id: str | None = None,
        job_id: str | None = None, from_state: IncidentState | None = None,
        to_state: IncidentState | None = None, details: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            """INSERT INTO audit_events
               (event_type,correlation_id,incident_id,job_id,from_state,to_state,details,occurred_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (event_type, correlation_id, incident_id, job_id, from_state, to_state,
             json.dumps(details or {}, sort_keys=True, separators=(",", ":")),
             _iso(occurred_at)),
        )

    @staticmethod
    def _recovery_from_row(row: sqlite3.Row) -> RecoveryJob:
        return IncidentRepository._recovery_from_mapping(dict(row))

    @staticmethod
    def _recovery_from_mapping(row: dict[str, Any]) -> RecoveryJob:
        return RecoveryJob(
            row["id"], row["incident_id"], row["issue_id"],
            RecoveryAction(row["action"]), row["status"], row["attempt_count"],
            row["correlation_id"], row["lease_owner"], row["lease_token"],
            bool(row["ambiguous"]), row["verification_attempt_count"],
        )
