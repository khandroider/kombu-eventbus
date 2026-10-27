"""Offline: inbound type -> Celery task."""
import pytest

from kombu_eventbus import Envelope, Requeue
from kombu_eventbus.celery import CeleryRouter, Route


class FakeCelery:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def send_task(self, name, *, kwargs, queue):
        if self.fail:
            raise ConnectionError("redis down")
        self.sent.append((name, kwargs, queue))


ROUTES = {
    "resume_parse_requested": Route(
        task="/resume_parser_v2/parse",
        queue="resume_parser_v2",
        kwargs=lambda p: {"request_body": {"request_id": p["requestId"], "file_url": p["fileUrl"]}},
    ),
}


def envelope(event_type="resume_parse_requested", **payload):
    return Envelope(type=event_type, payload=payload, source="taphr")


def test_known_type_is_sent_to_its_queue():
    app = FakeCelery()
    CeleryRouter(app, ROUTES)(envelope(requestId="r1", fileUrl="https://s3/cv.pdf"))
    assert app.sent == [("/resume_parser_v2/parse", {"request_body": {"request_id": "r1", "file_url": "https://s3/cv.pdf"}}, "resume_parser_v2")]


@pytest.mark.parametrize("env", [envelope("something_else", requestId="r1"), envelope(requestId="r1")])
def test_unmapped_type_or_bad_payload_is_dropped_quietly(env):
    app = FakeCelery()
    CeleryRouter(app, ROUTES)(env)  # no exception: the bus acks it
    assert app.sent == []


def test_broker_failure_asks_for_requeue():
    with pytest.raises(Requeue):
        CeleryRouter(FakeCelery(fail=True), ROUTES)(envelope(requestId="r1", fileUrl="u"))
