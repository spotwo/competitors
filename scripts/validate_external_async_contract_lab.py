#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "misc" / "external-async-contract-lab"
CONTRACT = LAB / "contract.yml"
ASYNCAPI = LAB / "asyncapi.yaml"
SAMPLE = LAB / "sample-event.json"
EVIDENCE = ROOT / "research" / "external-async-integration" / "evidence.yml"


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def signature(secret: bytes, message_id: str, timestamp: str, raw_body: bytes) -> str:
    signed = message_id.encode() + b"." + timestamp.encode() + b"." + raw_body
    digest = hmac.new(secret, signed, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode("ascii")


def verify_signature(secret: bytes, message_id: str, timestamp: str, raw_body: bytes, supplied: str) -> bool:
    return hmac.compare_digest(signature(secret, message_id, timestamp, raw_body), supplied)


def classify_http_status(status: int) -> str:
    if 200 <= status <= 299:
        return "success"
    if status in {408, 425, 429} or 500 <= status <= 599:
        return "retry"
    if 400 <= status <= 499:
        return "terminal"
    raise ValueError(f"unsupported HTTP status: {status}")


def validate() -> list[str]:
    errors: list[str] = []
    contract = load_yaml(CONTRACT)
    asyncapi = load_yaml(ASYNCAPI)
    event = json.loads(SAMPLE.read_text(encoding="utf-8"))
    evidence = load_yaml(EVIDENCE)

    profile = contract.get("profile", {})
    envelope = profile.get("envelope", {})
    delivery = profile.get("delivery", {})
    consumer = profile.get("consumer", {})
    security = profile.get("security", {})
    isolation = profile.get("isolation", {})

    if contract.get("status") != "trial":
        errors.append("contract status must remain trial until production delivery policy is decided")
    if envelope.get("standard") != "CloudEvents" or envelope.get("specversion") != "1.0":
        errors.append("lab must use the CloudEvents 1.0 event model")
    if profile.get("description", {}).get("version") != "3.1.0":
        errors.append("lab must describe the asynchronous API with AsyncAPI 3.1.0")
    if delivery.get("mechanism") != "HTTPS webhook":
        errors.append("lab primary delivery mechanism must be HTTPS webhook")
    if delivery.get("semantics") != "at-least-once":
        errors.append("external webhook delivery must model duplicate-capable at-least-once semantics")
    if consumer.get("duplicate_tolerance") != "required" or consumer.get("ordering_assumption") != "none":
        errors.append("consumer contract must require duplicate tolerance and forbid global ordering assumptions")
    if not security.get("verify_raw_body"):
        errors.append("signature verification must use the exact raw request body")
    if isolation.get("internal_nats_topology_exposed") is not False:
        errors.append("public external contract must not expose internal NATS topology")

    required = set(envelope.get("required_attributes", []))
    missing = sorted(required - set(event))
    if missing:
        errors.append("sample CloudEvent is missing required attributes: " + ", ".join(missing))
    if event.get("specversion") != "1.0":
        errors.append("sample event specversion must be 1.0")
    if not str(event.get("type", "")).startswith("com.spotwo."):
        errors.append("sample event type must use the Spotwo-owned public event namespace")
    if event.get("datacontenttype") != "application/json":
        errors.append("sample data content type must be application/json")

    raw_body = SAMPLE.read_bytes()
    secret = b"external-async-contract-lab-secret"
    message_id = event["id"]
    timestamp = "1787161200"
    supplied = signature(secret, message_id, timestamp, raw_body)
    if not verify_signature(secret, message_id, timestamp, raw_body, supplied):
        errors.append("valid HMAC signature did not verify")
    if verify_signature(secret, message_id, timestamp, raw_body + b" ", supplied):
        errors.append("mutated body must not verify against the original signature")
    if delivery.get("stable_delivery_id") != "cloudevents.id" or consumer.get("idempotency_key") != "cloudevents.id":
        errors.append("CloudEvents id must remain stable across retries and be usable for consumer deduplication")

    expected_statuses = {
        200: "success",
        204: "success",
        408: "retry",
        425: "retry",
        429: "retry",
        500: "retry",
        503: "retry",
        400: "terminal",
        404: "terminal",
    }
    for status, expected in expected_statuses.items():
        if classify_http_status(status) != expected:
            errors.append(f"HTTP {status} must classify as {expected}")

    if asyncapi.get("asyncapi") != "3.1.0":
        errors.append("AsyncAPI document must use version 3.1.0")
    operation = asyncapi.get("operations", {}).get("deliverExternalBusinessEvent", {})
    if operation.get("action") != "send":
        errors.append("AsyncAPI operation must describe Spotwo sending the event")
    channel_ref = operation.get("channel", {}).get("$ref")
    if channel_ref != "#/channels/externalBusinessEvents":
        errors.append("AsyncAPI send operation must reference the external business event channel")

    public_artifacts = "\n".join(
        [
            ASYNCAPI.read_text(encoding="utf-8"),
            SAMPLE.read_text(encoding="utf-8"),
        ]
    ).casefold()
    for token in isolation.get("forbidden_public_tokens", []):
        if token.casefold() in public_artifacts:
            errors.append(f"public lab artifact leaks forbidden internal topology token: {token!r}")

    source_ids = {source.get("id") for source in evidence.get("sources", [])}
    required_sources = {
        "cloudevents-spec",
        "asyncapi-3-1",
        "standard-webhooks-1-0",
        "github-webhook-signatures",
        "stripe-webhook-delivery",
        "svix-webhooks",
        "convoy-webhooks",
        "manhattan-datastream-pubsub",
        "oracle-wms-integration",
    }
    absent_sources = sorted(required_sources - source_ids)
    if absent_sources:
        errors.append("research evidence is missing required sources: " + ", ".join(absent_sources))

    if not contract.get("unresolved_before_adopt"):
        errors.append("trial must preserve explicit unresolved production questions")

    return errors


def main() -> int:
    errors = validate()
    if errors:
        print("External async contract lab validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("External async contract lab validation passed: signed CloudEvents webhook trial")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
