# External async contract lab

This lab tests a public asynchronous integration shape without exposing Spotwo's internal event transport.

## Trial profile

```text
Spotwo canonical domain fact
        |
        | public projection
        v
CloudEvents 1.0 structured JSON
        |
        | signed HTTPS webhook
        | Standard Webhooks-style metadata
        v
Customer endpoint

AsyncAPI 3.1.0 describes the message-driven contract.
```

The layers are intentionally separate:

- CloudEvents defines the public event envelope and stable event metadata.
- HTTPS webhook is the delivery mechanism under trial.
- Standard Webhooks provides the signing/idempotency metadata shape used by the lab.
- AsyncAPI describes the asynchronous API contract for tooling and documentation.
- Spotwo owns durable delivery state, retry policy, replay, authorization, and customer diagnostics.

None of these artifacts expose NATS subjects, stream names, durable consumer identities, or internal retention settings.

## What the executable lab proves

Run:

```bash
python scripts/validate_external_async_contract_lab.py
python -m unittest scripts.test_external_async_contract_lab
```

The lab checks:

- required CloudEvents metadata;
- a Spotwo-owned public event type namespace;
- HMAC-SHA256 signing over message id, timestamp, and the exact raw body;
- signature failure when id, timestamp, or body changes;
- stable CloudEvents id as the consumer deduplication key;
- duplicate-capable at-least-once delivery semantics;
- retry classification for network-like HTTP failures and rate limiting;
- no global event-ordering assumption;
- an AsyncAPI 3.1.0 send operation;
- public artifact isolation from internal event-broker topology;
- presence of standards, open-source, operational, and commercial primary evidence.

## What it does not prove

This is not yet a production webhook service. It does not implement a durable delivery database, background workers, endpoint management, a replay API, customer dashboards, real network timeouts, rate limits, secret rotation, or a pull-feed recovery surface.

Those omissions are deliberate. The result of this slice is a bounded trial profile, not adoption.

## Evidence

`research/external-async-integration/evidence.yml` captures the primary-source evidence used for this lab. Research targets and verified claims must remain separate.
