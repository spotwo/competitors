# Tenant-aware Event Routing and Authorization — opt-in lab contract

Status: **tenant-scoped DB claim and real authenticated NATS ACL lab proof; NOT a production credential/ACL rollout**.

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
- SQL migration `033_tenant_scoped_outbox_claim.sql` adds
  `delivery_route` with `shared` default and only `shared` events remain
  claimable by the legacy publisher. `PostgresTenantOutboxStore` claims only
  `delivery_route='tenant'`, one specific tenant, one exact V2 posting type.
- Route transitions are **explicit**, one event at a time, and audited in
  `domain_event_outbox_route_actions`. `stage_tenant_event_route`
  refuses a currently claimed/leased, published, quarantined, V1 or wrong-tenant
  event. Its execution privilege and the tenant-claim function's are revoked
  from PostgreSQL `PUBLIC`; separately managed roles require explicit GRANT.
- The Outbox CLI can opt in with `--tenant-id <canonical UUID> --transport nats`.
  Omitting it retains the unchanged global publisher mode; do not use a
  global-claim adapter with a tenant-bound transport.
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

## Authenticated broker laboratory

The lab Docker Compose now starts a **separate** `nats-auth` server
with intentionally public synthetic credentials from
`nats-tenant-auth.conf`. Its fixed stream `WMS_TENANT_AUTH` uses
two pre-provisioned tenant durables and isolated NATS users:

- `lab_tenant_a_pub` and `lab_tenant_b_pub`: publish exactly their
  own tenant event subject, subscribe only to `_INBOX.>` for JetStream PUB ACK.
- `lab_tenant_a_sub` and `lab_tenant_b_sub`: publish only their
  own durable INFO / NEXT / ACK control subjects, subscribe only to
  reply inboxes, not to the other tenant's event subjects.
- `lab_admin`: unrestricted **local lab provisioner only**.

The tests check real broker `Permissions Violation` errors for forbidden
cross-tenant publish and subscribe, other-durable INFO, and consumer DELETE
control operations. They also prove the authorized JetStream PUB ACK, durable
pull, Inbox projection, ACK, and broker stream message count. Credentials
are publicly documented **test data** and MUST NOT be used for deployment.

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
2. **Implemented in lab:** stage selected unclaimed V2 postings into
   `delivery_route='tenant'` with an audit record, use the scoped Outbox
   claim for one authorized tenant, keep legacy/unstaged types on the shared
   publisher. Production still needs role provisioning and orchestrated
   operator approval for staging.
3. **Authenticated fixture proven:** distinct user credentials, exact
   broker publish grants and tenant-durable JetStream INFO/NEXT/ACK privileges,
   with real negative authorization tests. Production still needs managed
   secrets, least-privilege provisioning, rotation and account/tenant
   onboarding.
4. **Implemented as a disabled pilot:** `tenant_deployments` in the
   registry/schema declares exact tenant, separate business/canary durable
   identities and credentials by env **name**. The tenant topology
   provisioner dry-runs first, requires an explicit operator/approval reference,
   only creates missing exact-filter durables on an existing stream, and refuses
   drift. Readiness fails closed without current authenticated ACL denials,
   scoped PUB/PULL/ACK canary and exact topology. Production still requires
   a credential manager, durable operator audit, enabled runtime orchestration,
   authorized canary permissions and live SLI/watchdog integration.
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


## Canary ACL and append-only activation receipt

The isolated authenticated NATS fixture now grants tenant A's **publisher**
an exact `<prefix>.tenants.<tenant_id>.health_check.ping` heartbeat subject,
and grants a **dedicated** canary consumer only the corresponding durable
INFO/NEXT/ACK JetStream API subjects. The business consumer remains separate.
The tenant deployment probe confirms a real scoped PUB ACK, PULL and ACK;
the readiness gate rejects missing, stale or cross-tenant evidence.

`sql/034_tenant_deployment_activation_journal.sql` adds an append-only
activation journal and single-active-deployment receipt state. Its recorder
accepts a scoped operator/approval reference only after the controlled CLI
has verified readiness. The journal captures exact resource names and proof
timestamps; approval replay with identical scope is idempotent, another
approval or changed scope fails closed, and recorded journal history cannot
be updated or deleted by normal SQL. The function is revoked from
PostgreSQL `PUBLIC` and must be explicitly granted to a tightly scoped
activation role by the operator.

`--mode activate` does NOT enable registry entries and will reject the
shipped disabled pilot. Tests use an enabled in-memory copy and ephemeral
NATS/PostgreSQL fixtures to prove positive activation and rollback. A
supplied `approval-id` is NOT cryptographic or external attestation of
approval. Production still needs an independent approval/identity workflow,
managed broker/DB roles, runtime route cutover, health SLIs and rollback.

## Scoped publisher invocation (after fenced staging)

The following is a **shape example**, not a recommendation to run against an
unreviewed live broker. The operator must have first staged a particular V2
posting via `SELECT kernel_lab.stage_tenant_event_route(event_id, tenant_id,
operator_id)` using a privileged DB role. Do not stage claimed, quarantined,
published or V1 records.

```bash
KERNEL_LAB_DATABASE_URL=postgresql://... \
KERNEL_LAB_NATS_URL=nats://<tenant-publisher-credentials>@broker:4222 \
python bin/run-kernel-outbox-publisher \
  --transport nats --tenant-id 00000000-0000-0000-0000-000000000001 \
  --nats-stream WMS_EVENTS --nats-subject-prefix spotwo.wms.events \
  --once
```

Existing publishers without `--tenant-id` continue to claim only shared
rows. Tenant-route staging is deliberately not added as an automatic runtime
action, preventing accidental overlapping routes. The three current registry
pipelines are unchanged.

**Remaining production boundary:** these tests exercise static credentials
on a dedicated ephemeral broker; they do not provision customer credentials,
enforce DB row-level security, manage tenant-specific consumer topology in
the deployment registry, or switch the desired-state pipelines automatically.
