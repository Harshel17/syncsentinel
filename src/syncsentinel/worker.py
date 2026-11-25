from __future__ import annotations

import argparse
import os
import time
from uuid import uuid4

from syncsentinel.domain import utc_now
from syncsentinel.gateway import HttpStacksyncGateway
from syncsentinel.main import settings
from syncsentinel.services import ReconciliationEngine, RecoveryEngine


def run_tick(worker_id: str, repository=None, gateway=None) -> None:
    if repository is None:
        repository, _ = settings()
    tick_time = utc_now()
    repository.recover_expired_leases(
        tick_time, f"lease-sweep:{worker_id}:{uuid4()}"
    )
    repository.schedule_due_targets(tick_time)
    if gateway is None:
        gateway = HttpStacksyncGateway(
            os.getenv("STACKSYNC_BASE_URL", "http://127.0.0.1:8001"),
            os.getenv("STACKSYNC_API_KEY", ""),
        )
    reconciliation = ReconciliationEngine(repository, gateway)
    recovery = RecoveryEngine(repository, gateway)
    for engine in (
        reconciliation.run_once,
        recovery.run_recovery_once,
        recovery.run_verification_once,
    ):
        while True:
            try:
                if not engine(worker_id, utc_now()):
                    break
            except Exception:
                # A malformed response or lost lease is isolated to its durable job.
                # The next tick can continue other work and recover expired leases.
                break


def main() -> None:
    parser = argparse.ArgumentParser(description="Run durable SyncSentinel workers")
    parser.add_argument("--once", action="store_true", help="run one deterministic tick")
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()
    worker_id = f"worker-{os.getpid()}"
    repository, _ = settings()
    started_at = utc_now()
    recovered = repository.recover_expired_leases(
        started_at, f"restart:{worker_id}:{uuid4()}"
    )
    repository.record_worker_started(worker_id, started_at, recovered)
    while True:
        run_tick(worker_id, repository)
        if args.once:
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
