# Spotwo Architecture Boundary Map

> Generated from `architecture/boundaries.yml`. Do not edit manually.

This map defines ownership and contracts at architecture boundaries. A technology name is never allowed to define the domain model implicitly.

## Summary

- Boundaries: **10**
- Canonical contracts: **2**
- Candidate contracts: **7**
- Unresolved contracts: **1**
- Explicit research gaps: **20**

## Boundary matrix

| Boundary | Classification | From | To | Contract | Status | Decision | Gaps |
|---|---|---|---|---|---|---|---:|
| **External business API** (`external-business-api`) | `external-interoperability` | ERP / OMS / TMS / customer applications / partner systems | Spotwo application and domain layer | REST + OpenAPI 3.1 | `canonical` | `rest-openapi` | 2 |
| **Internal domain event backbone** (`internal-domain-event-backbone`) | `canonical-internal` | Spotwo domain producers | Spotwo domain consumers, projections, warehouse services, Edge-capable consumers | Broker-neutral domain events transported by NATS JetStream | `canonical` | `nats-jetstream` | 1 |
| **Device and machine telemetry** (`device-telemetry`) | `industrial-adapter-boundary` | Sensors, machines, PLC gateways, and Edge adapters | Spotwo telemetry and observability layer | MQTT with Sparkplug state conventions | `candidate` | `sparkplug-mqtt` | 2 |
| **Real versus simulated execution** (`execution-simulation`) | `internal-port` | Spotwo execution semantics | Real executor or simulation executor | Spotwo-owned executor and clock abstractions; Ocava as architecture reference/prototype input | `candidate` | `ocado-ocava` | 2 |
| **External asynchronous integration** (`external-async-integration`) | `external-interoperability` | Spotwo domains | Customer and partner systems outside the Spotwo trust domain | CloudEvents 1.0 structured JSON over signed HTTPS webhooks, described with AsyncAPI 3.1.0 | `candidate` | `signed-cloudevents-webhooks` | 3 |
| **Optimization and solver boundary** (`optimization-engine`) | `internal-port` | Spotwo planning and orchestration domains | Optimization solver implementation | Spotwo-owned serializable problem/result port with OR-Tools as the first baseline solver | `candidate` | `google-or-tools` | 2 |
| **PLC and machine control** (`plc-control`) | `industrial-adapter-boundary` | Spotwo WCS / MFC / Edge control adapter | PLCs and machine controllers | Spotwo-owned PLC port with PLC4X, OPC UA, or vendor-native protocol adapters | `candidate` | `apache-plc4x` | 2 |
| **AMR and AGV fleet control** (`robot-fleet-control`) | `robotics-adapter-boundary` | Spotwo WES / mission orchestration | Robot fleet manager or compatible AMR / AGV controller | Spotwo-owned robot adapter with VDA 5050 where supported and vendor-native adapters otherwise | `candidate` | `vda-5050` | 2 |
| **Supply-chain traceability and visibility** (`supply-chain-visibility`) | `external-standard-projection` | Spotwo canonical domain events and state | External supply-chain visibility consumers | GS1 EPCIS projection and query interoperability | `candidate` | `gs1-epcis` | 2 |
| **Spotwo Edge control and synchronization** (`edge-control-and-sync`) | `internal-adapter-boundary` | Spotwo cloud or site control plane | Spotwo Edge runtime at the warehouse | Unselected command, configuration, synchronization, and health contract | `unresolved` | - | 2 |

## System flow

```text
External ERP / OMS / TMS / customers
        | REST + OpenAPI 3.1
        v
Spotwo application + canonical domain
        |-- NATS JetStream domain events --> internal consumers / Edge consumers
        |-- signed CloudEvents webhook candidate --> customers / partners
        |-- GS1 EPCIS candidate ---------> visibility consumers
        |-- OR-Tools candidate ----------> optimization
        |-- real/sim executor port ------> execution
        v
Spotwo WES / WCS / MFC / Edge
        |-- VDA 5050 candidate ----------> robot fleet
        |-- PLC4X / OPC UA candidate ----> PLC / machine
        |<- MQTT / Sparkplug candidate --- devices / gateways
        |<-> [UNRESOLVED] Edge control --- control plane
        v
Physical world
```

## Rule of interpretation

`canonical` means the contract mechanism is adopted for this exact boundary. `candidate` means the boundary and ownership are defined but the mechanism is still under trial. `unresolved` means the map deliberately refuses to guess a mechanism. Detailed state ownership, reliability, security, versioning, simulation rules, evidence, standards, and research gaps live in `architecture/boundaries.yml`.
