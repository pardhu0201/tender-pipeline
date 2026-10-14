"""Notifications to a Slack-compatible incoming webhook.

Slack, Mattermost and Google Chat accept {"text": ...} directly; for Discord,
append /slack to the webhook URL. Alerting must never break a run, so every
failure here is logged and swallowed.
"""

from __future__ import annotations

import logging

import requests

from tender_pipeline.config import Settings

log = logging.getLogger(__name__)


def send_alert(settings: Settings, title: str, details: str = "") -> bool:
    if not settings.alert_webhook_url:
        return False
    text = f"{title}\n{details}".strip()
    try:
        response = requests.post(settings.alert_webhook_url, json={"text": text}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        log.warning("could not send alert: %s", exc)
        return False
    return True
