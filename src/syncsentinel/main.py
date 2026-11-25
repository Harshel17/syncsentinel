from __future__ import annotations

import os

from syncsentinel.app import create_app
from syncsentinel.domain import utc_now
from syncsentinel.persistence import IncidentRepository, SyncTarget


def settings() -> tuple[IncidentRepository, SyncTarget]:
    repository = IncidentRepository(os.getenv("SYNCSENTINEL_DB", "syncsentinel.sqlite3"))
    repository.initialize()
    target = SyncTarget(
        workspace_id=int(os.getenv("STACKSYNC_WORKSPACE_ID", "42")),
        region=os.getenv("STACKSYNC_REGION", "usnv"),
        base_id=os.getenv(
            "STACKSYNC_BASE_ID", "3f6c1d2e-8a4b-4c7d-9e21-5b6f7a8c9d01"
        ),
        poll_interval_seconds=int(os.getenv("RECONCILIATION_INTERVAL_SECONDS", "60")),
    )
    repository.register_target(target, utc_now())
    return repository, target


def create_application():
    repository, _ = settings()
    webhook_secret = os.getenv("STACKSYNC_WEBHOOK_SECRET")
    if not webhook_secret:
        raise RuntimeError("STACKSYNC_WEBHOOK_SECRET must be configured")
    return create_app(
        repository,
        webhook_bearer_secret=webhook_secret,
    )
