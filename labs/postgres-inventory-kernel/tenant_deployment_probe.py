"""Active broker ACL and independent tenant-canary evidence for readiness."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4, uuid5

import nats

from tenant_event_deployment import TenantDeploymentBinding


class TenantDeploymentProbe:
    def __init__(
        self, *, publisher_nats_url: str, consumer_nats_url: str,
        canary_consumer_nats_url: str,
    ):
        urls = (publisher_nats_url, consumer_nats_url, canary_consumer_nats_url)
        if any(not x or not x.strip() for x in urls) or len(set(urls)) != 3:
            raise ValueError("three distinct scoped NATS credential URLs are required")
        self.urls = urls

    def run(self, binding: TenantDeploymentBinding) -> tuple[dict, dict]:
        with asyncio.Runner() as runner:
            acl_ok, canary_ok = runner.run(self._check(binding))
        timestamp = datetime.now(timezone.utc).isoformat()
        deployment = binding.deployment
        return (
            {
                "verified": acl_ok, "tenant_id": str(deployment.tenant_id),
                "stream": binding.stream, "durable": deployment.business_durable,
                "observed_at": timestamp,
            },
            {
                "verified": canary_ok, "tenant_id": str(deployment.tenant_id),
                "stream": binding.stream, "durable": deployment.canary_durable,
                "subject": binding.canary_subject, "observed_at": timestamp,
            },
        )

    async def _check(self, binding: TenantDeploymentBinding) -> tuple[bool, bool]:
        pub_errors: list[str] = []
        sub_errors: list[str] = []

        async def pub_error(error: Exception) -> None:
            pub_errors.append(str(error).lower())

        async def sub_error(error: Exception) -> None:
            sub_errors.append(str(error).lower())

        publisher = await nats.connect(
            servers=[self.urls[0]], error_cb=pub_error,
            allow_reconnect=False, connect_timeout=2,
        )
        subscriber = None
        canary = None
        try:
            subscriber = await nats.connect(
                servers=[self.urls[1]], error_cb=sub_error,
                allow_reconnect=False, connect_timeout=2,
            )
            canary = await nats.connect(
                servers=[self.urls[2]], allow_reconnect=False, connect_timeout=2,
            )
            tenant_id = binding.deployment.tenant_id
            prefix = binding.parent.transport.subject_prefix
            foreign_subject = (
                f"{prefix}.tenants.{uuid5(tenant_id, 'acl-negative-probe')}."
                "inventory.transaction.posted"
            )
            await publisher.publish(foreign_subject, b"ACL probe")
            await publisher.publish(
                f"{prefix}.inventory.transaction.posted", b"ACL probe",
            )
            await publisher.flush()
            await subscriber.subscribe(foreign_subject)
            await subscriber.publish(
                f"$JS.API.CONSUMER.INFO.{binding.stream}.FOREIGN_DURABLE", b"{}",
            )
            await subscriber.publish(
                f"$JS.API.CONSUMER.DELETE.{binding.stream}."
                f"{binding.deployment.business_durable}", b"{}",
            )
            await subscriber.flush()
            await asyncio.sleep(0.2)
            acl_ok = (
                sum("permissions violation for publish" in x for x in pub_errors) >= 2
                and any("permissions violation for subscription" in x for x in sub_errors)
                and any("$js.api.consumer.info" in x for x in sub_errors)
                and any("$js.api.consumer.delete" in x for x in sub_errors)
            )
            nonce = str(uuid4()).encode("ascii")
            try:
                receipt = await publisher.jetstream().publish(
                    binding.canary_subject, nonce, stream=binding.stream, timeout=2,
                )
                sub = await canary.jetstream().pull_subscribe_bind(
                    stream=binding.stream,
                    durable=binding.deployment.canary_durable,
                )
                message = (await sub.fetch(batch=1, timeout=2))[0]
                canary_ok = (
                    receipt.stream == binding.stream
                    and message.subject == binding.canary_subject
                    and message.data == nonce
                )
                if canary_ok:
                    await message.ack_sync(timeout=2)
            except Exception:
                canary_ok = False
            return acl_ok, canary_ok
        finally:
            if canary is not None:
                await canary.close()
            if subscriber is not None:
                await subscriber.close()
            await publisher.close()
