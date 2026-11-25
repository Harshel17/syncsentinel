from __future__ import annotations

import os
from uuid import uuid4

import httpx
from fastapi.middleware.cors import CORSMiddleware

from syncsentinel.app import create_app
from syncsentinel.domain import utc_now
from syncsentinel.gateway import HttpStacksyncGateway
from syncsentinel.main import settings
from syncsentinel.services import ReconciliationEngine
from syncsentinel.worker import run_tick


def create_demo_application():
    """Local-only integrated API for the React demonstration console."""
    webhook_secret = os.getenv("STACKSYNC_WEBHOOK_SECRET")
    if not webhook_secret:
        raise RuntimeError("STACKSYNC_WEBHOOK_SECRET must be configured")
    simulator_url = os.getenv("STACKSYNC_BASE_URL", "http://127.0.0.1:8001")
    repository, target = settings()
    gateway = HttpStacksyncGateway(simulator_url)
    control_client = httpx.Client(base_url=simulator_url, timeout=5)
    worker_id = f"integrated-demo-worker-{os.getpid()}"
    started = utc_now()
    recovered = repository.recover_expired_leases(
        started, f"demo-start:{uuid4()}"
    )
    repository.record_worker_started(worker_id, started, recovered)

    def worker_tick() -> None:
        run_tick(worker_id, repository, gateway)

    def reset() -> None:
        response = control_client.post("/simulator/control/reset")
        response.raise_for_status()
        repository.reset_demo_data(utc_now())

    def load_scenario(name: str) -> None:
        response = control_client.post(f"/simulator/control/scenarios/{name}")
        if response.status_code == 404:
            raise ValueError(name)
        response.raise_for_status()
        now = utc_now()
        repository.reset_demo_data(now)
        repository.register_target(target, now)
        repository.schedule_reconciliation(
            target,
            now,
            f"scenario:{name}:{uuid4()}",
            reason="scheduled_poll",
        )
        ReconciliationEngine(repository, gateway).run_once(worker_id, utc_now())

    app = create_app(
        repository,
        webhook_bearer_secret=webhook_secret,
        gateway=gateway,
        worker_tick=worker_tick,
        scenario_controller=load_scenario,
        reset_controller=reset,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )
    return app
