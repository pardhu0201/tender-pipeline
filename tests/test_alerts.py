import pytest
import requests

from tender_pipeline.alerts import send_alert
from tender_pipeline.config import Settings

WEBHOOK = Settings(database_url="", alert_webhook_url="https://hooks.example.com/T000/B000")


def test_no_webhook_configured_means_no_request(monkeypatch):
    monkeypatch.setattr(requests, "post", lambda *a, **k: pytest.fail("must not post"))
    assert send_alert(Settings(database_url=""), "run 7 FAILED") is False


def test_alert_is_slack_compatible_json(monkeypatch):
    sent = {}

    class Accepted:
        def raise_for_status(self):
            pass

    def fake_post(url, *, json, timeout):
        sent.update(url=url, json=json, timeout=timeout)
        return Accepted()

    monkeypatch.setattr(requests, "post", fake_post)
    assert send_alert(WEBHOOK, "run 7 FAILED", "GemError: simulated outage") is True
    assert sent["url"] == WEBHOOK.alert_webhook_url
    assert sent["json"] == {"text": "run 7 FAILED\nGemError: simulated outage"}
    assert sent["timeout"] == 10


def test_a_broken_webhook_never_breaks_the_run(monkeypatch):
    def unreachable(*args, **kwargs):
        raise requests.ConnectionError("webhook host is down")

    monkeypatch.setattr(requests, "post", unreachable)
    assert send_alert(WEBHOOK, "run 7 FAILED") is False
