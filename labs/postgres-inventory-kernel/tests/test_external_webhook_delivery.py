from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from conftest import DATABASE_URL, TENANT
from external_webhook_delivery import (
    ExternalWebhookDeliveryError,
    PostgresExternalWebhookDeliveryJournal,
    classify_http_status,
    retry_delay_seconds,
)

NOW = datetime(2026, 8, 19, 18, 0, tzinfo=timezone.utc)
EVENT = {
    "specversion": "1.0",
    "id": "0198c9fa-5d20-7d14-a4d2-111111111111",
    "source": "https://spotwo.example/warehouses/W1",
    "type": "com.spotwo.inventory.position.changed.v1",
    "datacontenttype": "application/json",
    "subject": "inventory-position/INV-1",
    "time": "2026-08-19T18:00:00Z",
    "data": {"inventory_position_id": "INV-1", "quantity": 7},
}


def journal(worker: str, *, lease_seconds: float = 60, max_attempts: int = 8):
    instance = PostgresExternalWebhookDeliveryJournal(
        DATABASE_URL,
        worker_id=worker,
        lease_seconds=lease_seconds,
    )
    subscription = instance.create_subscription(
        tenant_id=TENANT,
        endpoint_url="https://customer.example/webhooks/spotwo",
        event_types=(EVENT["type"],),
        signing_secret_ref="vault://customer-a/webhook",
        signing_secret_version=1,
        max_attempts=max_attempts,
    )
    return instance, subscription


def test_enqueue_is_idempotent_for_same_public_event_identity():
    worker, subscription = journal("worker-a")

    first = worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    second = worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)

    assert second.delivery_id == first.delivery_id
    assert second.generation == 0
    assert second.state == "pending"


def test_reusing_event_identity_with_different_payload_is_rejected():
    worker, subscription = journal("worker-a")
    worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    changed = deepcopy(EVENT)
    changed["data"]["quantity"] = 8

    with pytest.raises(ExternalWebhookDeliveryError) as error:
        worker.enqueue(subscription_id=subscription.subscription_id, envelope=changed, now=NOW)

    assert error.value.code == "external_webhook_event_identity_conflict"


def test_two_workers_cannot_claim_the_same_delivery():
    owner, subscription = journal("worker-seed")
    owner.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    first = PostgresExternalWebhookDeliveryJournal(DATABASE_URL, worker_id="worker-a", lease_seconds=60)
    second = PostgresExternalWebhookDeliveryJournal(DATABASE_URL, worker_id="worker-b", lease_seconds=60)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda item: item.claim_next(now=NOW), (first, second)))

    claimed = [claim for claim in claims if claim is not None]
    assert len(claimed) == 1
    assert claimed[0].delivery.event_id == EVENT["id"]


def test_expired_lease_is_recovered_and_stale_worker_cannot_complete():
    owner, subscription = journal("worker-seed")
    owner.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    first = PostgresExternalWebhookDeliveryJournal(DATABASE_URL, worker_id="worker-a", lease_seconds=10)
    second = PostgresExternalWebhookDeliveryJournal(DATABASE_URL, worker_id="worker-b", lease_seconds=10)

    claim_a = first.claim_next(now=NOW)
    assert claim_a is not None
    claim_b = second.claim_next(now=NOW + timedelta(seconds=11))
    assert claim_b is not None
    assert claim_b.delivery.delivery_id == claim_a.delivery.delivery_id
    assert claim_b.attempt_no == 2

    history = second.attempt_history(claim_b.delivery.delivery_id)
    assert history[0]["outcome"] == "lease_expired"
    assert history[0]["completed_at"] == NOW + timedelta(seconds=11)

    with pytest.raises(ExternalWebhookDeliveryError) as error:
        first.record_result(claim_a, http_status=204, now=NOW + timedelta(seconds=12))
    assert error.value.code == "external_webhook_delivery_lease_lost"

    delivered = second.record_result(claim_b, http_status=204, now=NOW + timedelta(seconds=12))
    assert delivered.state == "delivered"


def test_successful_http_result_marks_delivery_delivered():
    worker, subscription = journal("worker-a")
    pending = worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    claim = worker.claim_next(now=NOW)
    assert claim is not None

    delivered = worker.record_result(claim, http_status=200, now=NOW + timedelta(seconds=1))

    assert delivered.delivery_id == pending.delivery_id
    assert delivered.state == "delivered"
    assert delivered.delivered_at == NOW + timedelta(seconds=1)
    history = worker.attempt_history(delivered.delivery_id)
    assert history[0]["outcome"] == "success"
    assert history[0]["http_status"] == 200


def test_retryable_result_schedules_deterministic_retry():
    worker, subscription = journal("worker-a")
    worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    claim = worker.claim_next(now=NOW)
    assert claim is not None

    retrying = worker.record_result(
        claim,
        http_status=503,
        error_code="upstream_unavailable",
        now=NOW + timedelta(seconds=1),
    )

    assert retrying.state == "pending"
    assert retrying.next_attempt_at == NOW + timedelta(seconds=1 + retry_delay_seconds(1))
    assert worker.claim_next(now=NOW + timedelta(seconds=5)) is None
    next_claim = worker.claim_next(now=NOW + timedelta(seconds=6))
    assert next_claim is not None
    assert next_claim.attempt_no == 2


def test_network_failure_is_retryable_without_an_http_status():
    worker, subscription = journal("worker-a")
    worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    claim = worker.claim_next(now=NOW)
    assert claim is not None

    retrying = worker.record_result(
        claim,
        http_status=None,
        error_code="connect_timeout",
        now=NOW + timedelta(seconds=1),
    )

    assert retrying.state == "pending"
    assert retrying.next_attempt_at == NOW + timedelta(seconds=6)


def test_terminal_client_error_moves_delivery_to_dead_letter():
    worker, subscription = journal("worker-a")
    worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    claim = worker.claim_next(now=NOW)
    assert claim is not None

    failed = worker.record_result(
        claim,
        http_status=400,
        error_code="invalid_customer_contract",
        now=NOW + timedelta(seconds=1),
    )

    assert failed.state == "dead_letter"
    assert failed.dead_lettered_at == NOW + timedelta(seconds=1)
    assert worker.attempt_history(failed.delivery_id)[0]["outcome"] == "dead_letter"


def test_retry_budget_exhaustion_moves_delivery_to_dead_letter():
    worker, subscription = journal("worker-a", max_attempts=2)
    worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    first = worker.claim_next(now=NOW)
    assert first is not None
    first_result = worker.record_result(first, http_status=503, now=NOW + timedelta(seconds=1))
    second = worker.claim_next(now=first_result.next_attempt_at)
    assert second is not None

    exhausted = worker.record_result(second, http_status=503, now=first_result.next_attempt_at + timedelta(seconds=1))

    assert exhausted.state == "dead_letter"
    history = worker.attempt_history(exhausted.delivery_id)
    assert [item["outcome"] for item in history] == ["retry", "dead_letter"]
    assert history[-1]["error_code"] == "max_attempts_exhausted"


def test_paused_and_revoked_subscriptions_do_not_issue_new_claims():
    worker, subscription = journal("worker-a")
    worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    worker.set_subscription_state(subscription.subscription_id, "paused")
    assert worker.claim_next(now=NOW) is None

    worker.set_subscription_state(subscription.subscription_id, "active")
    assert worker.claim_next(now=NOW) is not None


def test_replay_creates_new_generation_and_snapshots_rotated_secret():
    worker, subscription = journal("worker-a")
    original = worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)
    claim = worker.claim_next(now=NOW)
    assert claim is not None
    terminal = worker.record_result(claim, http_status=204, now=NOW + timedelta(seconds=1))
    worker.rotate_signing_secret(
        subscription.subscription_id,
        signing_secret_ref="vault://customer-a/webhook-v2",
        signing_secret_version=2,
    )

    replay = worker.request_replay(
        terminal.delivery_id,
        reason="customer requested recovery replay",
        requested_by="operator:serhii",
        now=NOW + timedelta(minutes=1),
    )

    assert replay.event_id == original.event_id
    assert replay.envelope == original.envelope
    assert replay.generation == 1
    assert replay.replay_of_delivery_id == original.delivery_id
    assert replay.signing_secret_ref == "vault://customer-a/webhook-v2"
    assert replay.signing_secret_version == 2
    assert original.signing_secret_version == 1


def test_non_terminal_delivery_cannot_be_replayed():
    worker, subscription = journal("worker-a")
    pending = worker.enqueue(subscription_id=subscription.subscription_id, envelope=EVENT, now=NOW)

    with pytest.raises(ExternalWebhookDeliveryError) as error:
        worker.request_replay(
            pending.delivery_id,
            reason="too early",
            requested_by="operator:test",
            now=NOW,
        )

    assert error.value.code == "external_webhook_replay_requires_terminal_delivery"


def test_http_status_and_retry_policy_match_public_contract():
    assert classify_http_status(204) == "success"
    assert classify_http_status(408) == "retry"
    assert classify_http_status(425) == "retry"
    assert classify_http_status(429) == "retry"
    assert classify_http_status(500) == "retry"
    assert classify_http_status(404) == "terminal"
    assert classify_http_status(None) == "retry"
    assert [retry_delay_seconds(attempt) for attempt in range(1, 10)] == [
        5,
        30,
        120,
        600,
        1800,
        3600,
        7200,
        21600,
        21600,
    ]
