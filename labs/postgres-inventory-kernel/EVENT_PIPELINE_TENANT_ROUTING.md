# Tenant-aware Event Routing and Authorization — opt-in lab contract

Status: **implemented and lab-proven route guard; NOT a production ACL rollout**.

## Contract

For Event Contract V2 `inventory.transaction.posted` only:

```text
<subject_prefix>.tenants.<canonical-tenant-uuid>.inventory.transaction.posted
```

For example:

```text
spotwo.wms.events.tenants.00000000-0000-0000-0000-000000000001.inventory.transaction.posted
```

The exact tenant UUID must match in all three places:

1. NATS subject route.
2. `Spotwo-Tenant-Id` message header.
3. The V2 envelope top-level `tenant_id`.

The tenant-bound `NatsJetStreamTransport` validates the event schema,
type and canonical tenant before attempting any NATS publish. An opted-in
`NatsJetStreamPullSource` checks that its pre-provisioned durable uses an
**exact** matching subject filter, then validates subject/header/envelope on
every delivery before passing it to the Inbox handler. Mismatches are
classified `invalid_tenant_route` and go through the existing malformed
delivery handling path. No silent fallback to a shared filter.

**V1 is not eligible for tenant routing.** V1 lacks the tenant identity:
retry/replay of V1 requires the existing legacy subject and local fallback
until drained. V2 remains supported on legacy subjects while the routing
rollout is staged.

## Intentional scope and defaults

- The three existing desired-state pipelines in
  `operations/event-pipelines/registry.yml` keep their current shared-subject
  filters; changing those without provisioning would cause delivery outages.
- Tenant-scoped transport/source are explicit Python opt-in APIs; the
  unfiltered `PostgresOutboxStore` still claims a shared global Outbox.
  **Do not attach it to a tenant-only transport**: the transport correctly
  rejects unrelated V1/cross-tenant claims, but doing so would leave a mixed
  queue failing/retrying. A tenant-aware source claiming adapter is required
  before production enablement.
- The third projection handler can be instantiated with
  `expected_tenant_id` for in-process domain isolation. It is a second
  layer after the exact JetStream durable filter.
- This is not a change to Work State or Inventory Position event routes.
- Both NATS stream provisioning and security credentials remain outside the
  registry; routing tests use an ephemeral JetStream stream and durables only.

## Example of bounded tenant runtime

```python
from uuid import UUID

from event_tenant_routing import TenantEventRoute
from nats_transport import NatsJetStreamTransport
from nats_consumer import NatsJetStreamPullSource

route = TenantEventRoute(
    subject_prefix="spotwo.wms.events",
    tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
)

# Requires separate Outbox claim scoping before production operation.
publisher = NatsJetStreamTransport(
    server_url=nats_url,
    stream_name="WMS_EVENTS",
    subject_prefix=route.subject_prefix,
    tenant_route=route,
)

# Must bind to a separately provisioned, exact-filtered durable.
consumer = NatsJetStreamPullSource(
    server_url=nats_url,
    stream_name="WMS_EVENTS",
    durable_name=tenant_specific_durable,
    tenant_route=route,
)
```

## NATS broker ACL / credential contract — mandatory before activation

Subject routing or a tenant header **is not authentication**. The lab
NATS compose service uses no tenant credentials; it proves dispatch/filter
behavior, not isolation against an authenticated malicious broker client.

Production broker provisioning **must**:

1. Issue distinct credentials/accounts for publishers, tenant consumers and
   privileged provisioners; no tenant client may use an administrator URL.
2. Permit tenant publishers to publish only their reviewed tenant subject
   prefix and necessary scoped JetStream API requests. Deny another tenant's
   publish subject, the legacy shared business subject and broad `>` grants.
3. Restrict tenant consumers to one reviewed durable bound to the *exact*
   tenant subject; verify the applicable JetStream API, reply inbox, ACK
   subjects and stream metadata permissions for that durable. A simple
   `subscribe` ACL alone is insufficient for JetStream pull consumers.
4. Prevent tenant clients from changing or creating stream/consumer
   topology, or accessing any other tenant's durable/subject.
5. Add negative **broker-enforced** tests using distinct real credentials:
   unauthorized cross-tenant publish/subscribe, wildcard grants, and
   unauthorized JetStream API access must be denied by NATS.
6. Restrict the service's stored DB/connection credentials to the appropriate
   tenants/roles; review handling of request logs and poison/malformed
   evidence as tenant-sensitive data.

Do **not** claim broker-enforced tenant security until those credentials and
negative authorization tests are green in a dedicated authenticated NATS
environment.

## Application read authorization

`inventory_transaction_index_reader.list_authorized_transactions` requires
a `VerifiedPrincipal` obtained **only from a trusted upstream auth
middleware**. It denies missing `inventory.transactions.read` scope or an
unpermitted tenant before making any SQL query, then filters by
`(consumer_name, tenant_id)` and bounds result count. The principal model
is an application integration contract, not an identity-token verifier and
does not replace database row-level security.

## Production rollout gates (separate reviewed PRs)

1. Inventory all pending/outstanding V1 transaction posting events,
   replay/failure lanes, and historical consumers. Drain or reconcile each
   one; do not silently upcast old event IDs.
2. Implement an Outbox claim adapter that claims only authorized V2
   `inventory.transaction.posted` events for the tenant, while legacy and
   other business event types continue through their existing publishers.
3. Provision reviewed per-tenant JetStream consumer/durable, stream subjects
   and broker ACL credentials; verify the privilege boundary with real
   negative credential tests.
4. Add tenant partition identity and routing state to deployment registry,
   schema and readiness preflight; require exact topology and real canary
   results before switching consumers.
5. Perform a bounded dual-publish/dual-consume migration or a safely
   fenced cutover with monitored handoff. Never silently publish one event
   on two routes unless duplicate semantics are explicitly reviewed.
6. Verify read authz, replay isolation, malformed-lane tenant evidence
   retention, independent projection and rollback behavior before declaring
   service readiness.

## Lab proof

`tests/test_tenant_event_routing.py` exercises:

- deterministic tenant subjects and three-way identity mismatch rejection;
- publisher refusal of cross-tenant and V1 events before connecting;
- fail-closed durable filter validation;
- real NATS JetStream delivery with two tenants on one stream,
  exact subject filter, Inbox commit and no cross-tenant projection;
- attempted forged delivery classified as `invalid_tenant_route`;
- read authorization gating before SQL and tenant-keyed projection reads.

The existing V1 and V2 paths continue through their old tests.
