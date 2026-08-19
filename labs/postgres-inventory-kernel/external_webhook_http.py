from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import json
import math
import socket
import ssl
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Mapping, Protocol
from urllib.parse import urlsplit

from external_webhook_delivery import (
    ExternalEventDelivery,
    ExternalWebhookDeliveryClaim,
    PostgresExternalWebhookDeliveryJournal,
)

_MAX_RETRY_AFTER_SECONDS = 86_400


def canonical_cloud_event_body(envelope: Mapping[str, object]) -> bytes:
    """Return stable UTF-8 bytes for a structured CloudEvents JSON delivery."""
    return json.dumps(
        dict(envelope),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def sign_webhook(secret: bytes, message_id: str, timestamp: str, raw_body: bytes) -> str:
    if not secret:
        raise ValueError("secret must not be empty")
    signed = message_id.encode("utf-8") + b"." + timestamp.encode("ascii") + b"." + raw_body
    digest = hmac.new(secret, signed, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode("ascii")


def verify_webhook_signature(
    secret: bytes,
    message_id: str,
    timestamp: str,
    raw_body: bytes,
    supplied: str,
) -> bool:
    return hmac.compare_digest(sign_webhook(secret, message_id, timestamp, raw_body), supplied)


def parse_retry_after(
    value: str | None,
    *,
    now: datetime | None = None,
    max_seconds: int = _MAX_RETRY_AFTER_SECONDS,
) -> int | None:
    """Parse Retry-After delta-seconds or HTTP-date into a bounded delay."""
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    if max_seconds <= 0:
        raise ValueError("max_seconds must be positive")

    if raw.isdigit():
        return min(int(raw), max_seconds)

    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    seconds = max(
        0,
        math.ceil(
            (parsed.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds()
        ),
    )
    return min(seconds, max_seconds)


class WebhookSecretResolver(Protocol):
    def resolve(self, secret_ref: str, version: int) -> bytes:
        ...


class MappingWebhookSecretResolver:
    """Test/lab resolver that models an opaque versioned secret-manager boundary."""

    def __init__(self, secrets: Mapping[tuple[str, int], bytes]):
        self._secrets = dict(secrets)

    def resolve(self, secret_ref: str, version: int) -> bytes:
        secret = self._secrets.get((secret_ref, version))
        if secret is None:
            raise KeyError((secret_ref, version))
        if not secret:
            raise ValueError("resolved secret must not be empty")
        return secret


@dataclass(frozen=True)
class WebhookHttpResult:
    http_status: int | None
    error_code: str | None
    retry_after_seconds: int | None
    response_bytes: int
    elapsed_ms: int


@dataclass(frozen=True)
class WebhookDispatchOutcome:
    claim: ExternalWebhookDeliveryClaim
    transport: WebhookHttpResult
    delivery: ExternalEventDelivery


class HttpsWebhookClient:
    """Small HTTPS client used to exercise the public delivery boundary with real sockets/TLS."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 2.0,
        ssl_context: ssl.SSLContext | None = None,
        max_response_bytes: int = 65_536,
        user_agent: str = "Spotwo-Webhook-Lab/1",
    ):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        self.timeout_seconds = float(timeout_seconds)
        self.ssl_context = ssl_context or ssl.create_default_context()
        self.max_response_bytes = int(max_response_bytes)
        self.user_agent = user_agent

    @staticmethod
    def _error(error_code: str, started: float) -> WebhookHttpResult:
        return WebhookHttpResult(
            http_status=None,
            error_code=error_code,
            retry_after_seconds=None,
            response_bytes=0,
            elapsed_ms=max(0, int((time.monotonic() - started) * 1000)),
        )

    def deliver(
        self,
        claim: ExternalWebhookDeliveryClaim,
        *,
        secret: bytes,
        timestamp: str | None = None,
        retry_after_now: datetime | None = None,
    ) -> WebhookHttpResult:
        endpoint = urlsplit(claim.endpoint_url)
        if endpoint.scheme != "https" or not endpoint.hostname:
            raise ValueError("webhook endpoint must be an absolute https URL")
        if endpoint.username is not None or endpoint.password is not None:
            raise ValueError("webhook endpoint must not contain userinfo")

        raw_body = canonical_cloud_event_body(claim.delivery.envelope)
        timestamp = timestamp or str(int(time.time()))
        signature = sign_webhook(secret, claim.delivery.event_id, timestamp, raw_body)
        path = endpoint.path or "/"
        if endpoint.query:
            path += "?" + endpoint.query

        headers = {
            "Content-Type": "application/cloudevents+json",
            "User-Agent": self.user_agent,
            "webhook-id": claim.delivery.event_id,
            "webhook-timestamp": timestamp,
            "webhook-signature": signature,
        }

        connection = http.client.HTTPSConnection(
            endpoint.hostname,
            endpoint.port or 443,
            timeout=self.timeout_seconds,
            context=self.ssl_context,
        )
        started = time.monotonic()
        try:
            connection.request("POST", path, body=raw_body, headers=headers)
            response = connection.getresponse()
            response_body = response.read(self.max_response_bytes + 1)
            response_bytes = len(response_body)
            retry_after_seconds = parse_retry_after(
                response.getheader("Retry-After"),
                now=retry_after_now,
            )
            error_code = None if 200 <= response.status <= 299 else f"http_{response.status}"
            if response_bytes > self.max_response_bytes:
                error_code = error_code or "response_too_large"
            return WebhookHttpResult(
                http_status=response.status,
                error_code=error_code,
                retry_after_seconds=retry_after_seconds,
                response_bytes=response_bytes,
                elapsed_ms=max(0, int((time.monotonic() - started) * 1000)),
            )
        except ssl.SSLCertVerificationError:
            return self._error("tls_certificate_error", started)
        except ssl.SSLError:
            return self._error("tls_error", started)
        except (socket.timeout, TimeoutError):
            return self._error("timeout", started)
        except ConnectionResetError:
            return self._error("connection_reset", started)
        except http.client.RemoteDisconnected:
            return self._error("remote_disconnected", started)
        except ConnectionRefusedError:
            return self._error("connection_refused", started)
        except OSError:
            return self._error("network_error", started)
        finally:
            connection.close()


class ExternalWebhookHttpWorker:
    """Claim one durable delivery, resolve its snapshotted secret, and attempt HTTPS delivery."""

    def __init__(
        self,
        journal: PostgresExternalWebhookDeliveryJournal,
        *,
        client: HttpsWebhookClient,
        secret_resolver: WebhookSecretResolver,
    ):
        self.journal = journal
        self.client = client
        self.secret_resolver = secret_resolver

    def run_once(self, *, now: datetime | None = None) -> WebhookDispatchOutcome | None:
        claim = self.journal.claim_next(now=now)
        if claim is None:
            return None

        try:
            secret = self.secret_resolver.resolve(
                claim.delivery.signing_secret_ref,
                claim.delivery.signing_secret_version,
            )
        except (KeyError, ValueError):
            transport = WebhookHttpResult(
                http_status=None,
                error_code="signing_secret_unavailable",
                retry_after_seconds=None,
                response_bytes=0,
                elapsed_ms=0,
            )
        else:
            timestamp = None
            if now is not None:
                normalized = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
                timestamp = str(int(normalized.timestamp()))
            transport = self.client.deliver(
                claim,
                secret=secret,
                timestamp=timestamp,
                retry_after_now=now,
            )

        delivery = self.journal.record_result(
            claim,
            http_status=transport.http_status,
            error_code=transport.error_code,
            now=now,
        )
        return WebhookDispatchOutcome(
            claim=claim,
            transport=transport,
            delivery=delivery,
        )
