"""Turn inbound events into Celery tasks.

    router = CeleryRouter(celery_app, {
        "resume_parse_requested": Route(
            task="/resume_parser_v2/parse",
            queue="resume_parser_v2",
            kwargs=lambda p: {"request_body": {"request_id": p["requestId"], "file_url": p["fileUrl"]}},
        ),
    })
    bus.consume(router)

The task is sent by name, so the queue is part of the route: this process does
not import the task modules and Celery would otherwise use the default queue.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from .bus import Envelope, Requeue

__all__ = ["CeleryRouter", "Route"]

log = logging.getLogger("kombu_eventbus")


class _SendsTasks(Protocol):
    def send_task(self, name: str, *, kwargs: dict, queue: str) -> Any: ...


@dataclass(frozen=True)
class Route:
    task: str
    queue: str
    kwargs: Callable[[dict], dict]
    """payload -> kwargs for ``send_task``. A KeyError/TypeError here means the payload
    does not match the contract: the message is dropped with an error in the log."""


class CeleryRouter:
    def __init__(self, app: _SendsTasks, routes: Mapping[str, Route]) -> None:
        self.app = app
        self.routes = dict(routes)

    def __call__(self, envelope: Envelope) -> None:
        route = self.routes.get(envelope.type)
        if route is None:
            log.warning("[EVENTBUS] Unmapped inbound type %r skipped id=%s", envelope.type, envelope.id)
            return
        try:
            kwargs = route.kwargs(envelope.payload)
        except (KeyError, TypeError) as error:
            log.error("[EVENTBUS] %s id=%s payload is missing %s, dropped", envelope.type, envelope.id, error)
            return
        try:
            self.app.send_task(route.task, kwargs=kwargs, queue=route.queue)
        except Exception as error:  # noqa: BLE001 - Celery broker down: keep the message
            raise Requeue(f"could not enqueue {route.task}: {error}") from error
        log.info("[EVENTBUS] %s id=%s -> %s", envelope.type, envelope.id, route.task)
