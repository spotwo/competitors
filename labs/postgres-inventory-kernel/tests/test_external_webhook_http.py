from __future__ import annotations

import queue
import shutil
import socket
import ssl
import subprocess
import threading
import time
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from conftest import DATABASE_URL, TENANT
from external_webhook_delivery import PostgresExternalWebhookDeliveryJournal
from external_webhook_http import (
    ExternalWebhookHttpWorker,
    HttpsWebhookClient,
    MappingWebhookSecretResolver,
    canonical_cloud_event_body,
    parse_retry_after,
    verify_webhook_signature,
)

NOW = datetime(2026, 8, 19, 20, 0, tzinfo=timezone.utc)
EVENT = {
    "specversion": "1.0",
    "id": "0198ca62-2a20-7b7f-8b62-111111111111",
    "source": "https://spotwo.example/warehouses/W1",
    "type": "com.spotwo.inventory.position.changed.v1",
    "datacontenttype": "application/json",
    "subject": "inventory-position/INV-HTTP-1",
    "time": "2026-08-19T20:00:00Z",
    "data": {"inventory_position_id": "INV-HTTP-1", "quantity": 11},
}
SECRET_V1 = b"webhook-secret-v1"
SECRET_V2 = b"webhook-secret-v2"
REF_V1 = "vault://customer-http/webhook"
REF_V2 = "vault://customer-http/webhook-v2"


class _ScriptedHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.server.requests.append(
            {
                "path": self.path,
                "headers": {key.lower(): value for key, value in self.headers.items()},
                "body": body,
            }
        )
        action = self.server.actions.get(timeout=2)
        kind = action.get("kind", "status")

        if kind == "reset":
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return

        if kind == "slow":
            time.sleep(float(action.get("delay", 0.4)))

        status = int(action.get("status", 204))
        response_body = action.get("body", b"")
        if isinstance(response_body, str):
            response_body = response_body.encode("utf-8")

        try:
            self.send_response(status)
            for key, value in action.get("headers", {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            if response_body:
                self.wfile.write(response_body)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def log_message(self, format, *args):
        return


class _HttpsServer(ThreadingHTTPServer):
    daemon_threads = True


@contextmanager
def scripted_https_server(cert: Path, key: Path, actions: list[dict]):
    server = _HttpsServer(("127.0.0.1", 0), _ScriptedHandler)
    server.actions = queue.Queue()
    server.requests = []
    for action in actions:
        server.actions.put(action)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert, keyfile=key)
    server.socket = context.wrap_socket(server.socket, server_side=True)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture(scope="module")
def tls_material(tmp_path_factory):
    if shutil.which("openssl") is None:
        pytest.skip("openssl is required for the local HTTPS failure lab")

    directory = tmp_path_factory.mktemp("webhook-tls")
    cert = directory / "cert.pem"
    key = directory / "key.pem"
    config = directory / "openssl.cnf"
    config.write_text(
        """
[req]
distinguished_name = dn
x509_extensions = v3_req
prompt = no

[dn]
CN = localhost

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
""".strip()
        + "\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-config",
            str(config),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return cert, key


def trusted_context(cert: Path) -> ssl.SSLContext:
    return ssl.create_default_context(cafile=str(cert))


def journal_for(endpoint: str, worker_id: str = "http-worker"):
    journal = PostgresExternalWebhookDeliveryJournal(
        DATABASE_URL,
        worker_id=worker_id,
        lease_seconds=5,
    )
    subscription = journal.create_subscription(
        tenant_id=TENANT,
        endpoint_url=endpoint,
        event_types=(EVENT["type"],),
        signing_secret_ref=REF_V1,
        signing_secret_version=1,
        max_attempts=4,
    )
    return journal, subscription


def worker_for(journal, *, context: ssl.SSLContext, timeout: float = 0.5):
    resolver = MappingWebhookSecretResolver(
        {
            (REF_V1, 1): SECRET_V1,
            (REF_V2, 2): SECRET_V2,
        }
    )
    return ExternalWebhookHttpWorker(
        journal,
        client=HttpsWebhookClient(timeout_seconds=timeout, ssl_context=context),
        secret_resolver=resolver,
    )


def test_real_https_delivery_signs_exact_body_and_marks_delivered(tls_material):
    cert, key = tls_material
    with scripted_https_server(cert, key, [{"status": 204}]) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo?tenant=demo"
        journal, subscription = journal_for(endpoint)
        journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = worker_for(journal, context=trusted_context(cert))

        outcome = worker.run_once(now=NOW)

        assert outcome is not None
        assert outcome.transport.http_status == 204
        assert outcome.transport.error_code is None
        assert outcome.delivery.state == "delivered"
        assert len(server.requests) == 1
        request = server.requests[0]
        assert request["path"] == "/webhooks/spotwo?tenant=demo"
        assert request["body"] == canonical_cloud_event_body(EVENT)
        assert request["headers"]["content-type"] == "application/cloudevents+json"
        assert request["headers"]["webhook-id"] == EVENT["id"]
        assert verify_webhook_signature(
            SECRET_V1,
            EVENT["id"],
            request["headers"]["webhook-timestamp"],
            request["body"],
            request["headers"]["webhook-signature"],
        )


def test_untrusted_tls_certificate_is_retryable_and_never_reaches_handler(tls_material):
    cert, key = tls_material
    with scripted_https_server(cert, key, [{"status": 204}]) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = ExternalWebhookHttpWorker(
            journal,
            client=HttpsWebhookClient(timeout_seconds=0.5),
            secret_resolver=MappingWebhookSecretResolver({(REF_V1, 1): SECRET_V1}),
        )

        outcome = worker.run_once(now=NOW)

        assert outcome is not None
        assert outcome.transport.http_status is None
        assert outcome.transport.error_code == "tls_certificate_error"
        assert outcome.delivery.state == "pending"
        assert outcome.delivery.next_attempt_at == NOW + timedelta(seconds=5)
        assert server.requests == []


def test_429_retry_after_is_parsed_while_durable_retry_remains_bounded(tls_material):
    cert, key = tls_material
    with scripted_https_server(
        cert,
        key,
        [{"status": 429, "headers": {"Retry-After": "30"}}],
    ) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = worker_for(journal, context=trusted_context(cert))

        outcome = worker.run_once(now=NOW)

        assert outcome is not None
        assert outcome.transport.http_status == 429
        assert outcome.transport.error_code == "http_429"
        assert outcome.transport.retry_after_seconds == 30
        assert outcome.delivery.state == "pending"
        # PR 66 owns deterministic persistent retry scheduling. This lab captures the
        # remote backpressure hint without silently changing crash-safe journal semantics.
        assert outcome.delivery.next_attempt_at == NOW + timedelta(seconds=5)


def test_slow_consumer_times_out_without_blocking_retry_state(tls_material):
    cert, key = tls_material
    with scripted_https_server(
        cert,
        key,
        [{"kind": "slow", "delay": 0.35, "status": 204}],
    ) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = worker_for(journal, context=trusted_context(cert), timeout=0.1)

        outcome = worker.run_once(now=NOW)

        assert outcome is not None
        assert outcome.transport.http_status is None
        assert outcome.transport.error_code == "timeout"
        assert outcome.delivery.state == "pending"
        assert outcome.delivery.next_attempt_at == NOW + timedelta(seconds=5)


def test_connection_reset_is_retryable(tls_material):
    cert, key = tls_material
    with scripted_https_server(cert, key, [{"kind": "reset"}]) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = worker_for(journal, context=trusted_context(cert))

        outcome = worker.run_once(now=NOW)

        assert outcome is not None
        assert outcome.transport.http_status is None
        assert outcome.transport.error_code in {"connection_reset", "remote_disconnected", "network_error"}
        assert outcome.delivery.state == "pending"


def test_secret_rotation_changes_signature_for_real_https_replay(tls_material):
    cert, key = tls_material
    with scripted_https_server(cert, key, [{"status": 204}, {"status": 204}]) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        original = journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = worker_for(journal, context=trusted_context(cert))

        first = worker.run_once(now=NOW)
        assert first is not None
        assert first.delivery.state == "delivered"

        journal.rotate_signing_secret(
            subscription.subscription_id,
            signing_secret_ref=REF_V2,
            signing_secret_version=2,
        )
        replay = journal.request_replay(
            original.delivery_id,
            reason="customer recovery test",
            requested_by="operator:http-lab",
            now=NOW + timedelta(minutes=1),
        )
        assert replay.signing_secret_version == 2

        second = worker.run_once(now=NOW + timedelta(minutes=1))
        assert second is not None
        assert second.delivery.state == "delivered"
        assert len(server.requests) == 2

        first_request, second_request = server.requests
        for request in (first_request, second_request):
            assert request["headers"]["webhook-id"] == EVENT["id"]
            assert request["body"] == canonical_cloud_event_body(EVENT)

        first_signature = first_request["headers"]["webhook-signature"]
        second_signature = second_request["headers"]["webhook-signature"]
        first_timestamp = first_request["headers"]["webhook-timestamp"]
        second_timestamp = second_request["headers"]["webhook-timestamp"]

        assert verify_webhook_signature(
            SECRET_V1, EVENT["id"], first_timestamp, first_request["body"], first_signature
        )
        assert not verify_webhook_signature(
            SECRET_V2, EVENT["id"], first_timestamp, first_request["body"], first_signature
        )
        assert verify_webhook_signature(
            SECRET_V2, EVENT["id"], second_timestamp, second_request["body"], second_signature
        )
        assert not verify_webhook_signature(
            SECRET_V1, EVENT["id"], second_timestamp, second_request["body"], second_signature
        )


def test_missing_versioned_secret_is_retryable_without_sending(tls_material):
    cert, key = tls_material
    with scripted_https_server(cert, key, [{"status": 204}]) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        journal.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
        worker = ExternalWebhookHttpWorker(
            journal,
            client=HttpsWebhookClient(timeout_seconds=0.5, ssl_context=trusted_context(cert)),
            secret_resolver=MappingWebhookSecretResolver({}),
        )

        outcome = worker.run_once(now=NOW)

        assert outcome is not None
        assert outcome.transport.error_code == "signing_secret_unavailable"
        assert outcome.delivery.state == "pending"
        assert server.requests == []


def test_retry_after_supports_http_date_and_is_bounded():
    assert parse_retry_after("30", now=NOW) == 30
    assert parse_retry_after("999999", now=NOW) == 86_400
    assert parse_retry_after("Wed, 19 Aug 2026 20:01:00 GMT", now=NOW) == 60
    assert parse_retry_after("not-a-date", now=NOW) is None
    assert parse_retry_after(None, now=NOW) is None


def test_worker_run_once_claims_only_one_delivery_for_bounded_local_pressure(tls_material):
    cert, key = tls_material
    second_event = deepcopy(EVENT)
    second_event["id"] = "0198ca62-2a20-7b7f-8b62-222222222222"
    second_event["subject"] = "inventory-position/INV-HTTP-2"
    second_event["data"] = {"inventory_position_id": "INV-HTTP-2", "quantity": 12}

    with scripted_https_server(cert, key, [{"status": 204}, {"status": 204}]) as server:
        endpoint = f"https://localhost:{server.server_port}/webhooks/spotwo"
        journal, subscription = journal_for(endpoint)
        first_delivery = journal.enqueue(
            subscription_id=subscription.subscription_id,
            envelope=EVENT,
            now=NOW,
        )
        second_delivery = journal.enqueue(
            subscription_id=subscription.subscription_id,
            envelope=second_event,
            now=NOW,
        )
        worker = worker_for(journal, context=trusted_context(cert))

        first = worker.run_once(now=NOW)

        assert first is not None
        assert first.delivery.delivery_id in {first_delivery.delivery_id, second_delivery.delivery_id}
        assert len(server.requests) == 1

        other = PostgresExternalWebhookDeliveryJournal(
            DATABASE_URL,
            worker_id="http-worker-2",
            lease_seconds=5,
        ).claim_next(now=NOW)
        assert other is not None
        assert other.delivery.delivery_id != first.delivery.delivery_id
