# External webhook HTTP/TLS lab

This lab extends the durable delivery journal with a real HTTPS client and a scripted customer endpoint. It is executable evidence for the `external-async-integration` candidate boundary, not a production webhook service.

## What it exercises

`external_webhook_http.py` takes one durable claim from `PostgresExternalWebhookDeliveryJournal`, resolves the snapshotted signing-secret reference/version, serializes the stored CloudEvent deterministically, signs the exact bytes, performs an HTTPS POST, and feeds the transport result back into the existing durable journal.

The tests cover a successful trusted TLS path, default trust-store rejection of an untrusted certificate, response timeout, abrupt connection reset/disconnect, HTTP 429 with bounded `Retry-After` parsing, secret-resolution failure, versioned secret rotation across explicit replay, and bounded one-claim-per-worker invocation behavior.

## Run

```bash
bash bin/check-kernel-lab
```

The test certificate is generated locally with `openssl` and is valid only for the temporary test server. No certificate or private key is committed.

## Important boundary

The lab parses `Retry-After` but does not yet alter the durable PostgreSQL retry schedule. Applying the remote hint in a second transaction would weaken the crash-safe result-recording model from ADR 0064. Production support should extend the journal atomically before claiming that remote backpressure hints are durably honored.

Likewise, the in-memory mapping secret resolver models only the interface to a versioned secret manager. Production still needs endpoint ownership verification, SSRF/DNS-rebinding controls, scoped subscription authorization, a real secret manager, fleet-wide/per-subscription rate limiting, diagnostics, retention, observability/SLOs, and a pull-feed recovery decision.
