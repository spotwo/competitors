# Spotwo Architecture Reference Coverage

> Generated from `architecture/coverage-gaps.yml`. Do not edit manually.

This view turns the Architecture Boundary Map into a bounded research backlog. Evidence and future research targets are deliberately separate.

## Summary

- Boundaries: **10**
- Coverage: **2 covered**, **7 partial**, **1 missing**
- Priority: **4 P0**, **4 P1**, **2 P2**
- Explicit gaps: **32**
- Research targets: **54**

## Coverage matrix

| Priority | Boundary | Contract | Overall | Standards | Open source | Commercial | Executable | Gaps | Targets |
|---|---|---|---|---|---|---|---|---:|---:|
| `P0` | **External asynchronous integration** (`external-async-integration`) | `unresolved` | `missing` | `missing` | `missing` | `missing` | `missing` | 4 | 11 |
| `P0` | **Spotwo Edge control and synchronization** (`edge-control-and-sync`) | `unresolved` | `partial` | `missing` | `partial` | `missing` | `missing` | 4 | 8 |
| `P0` | **PLC and machine control** (`plc-control`) | `candidate` | `partial` | `partial` | `covered` | `missing` | `missing` | 4 | 12 |
| `P0` | **AMR and AGV fleet control** (`robot-fleet-control`) | `candidate` | `partial` | `partial` | `covered` | `missing` | `missing` | 3 | 7 |
| `P1` | **Device and machine telemetry** (`device-telemetry`) | `candidate` | `partial` | `partial` | `covered` | `missing` | `missing` | 4 | 4 |
| `P1` | **Real versus simulated execution** (`execution-simulation`) | `candidate` | `partial` | `missing` | `covered` | `missing` | `missing` | 4 | 4 |
| `P1` | **Optimization and solver boundary** (`optimization-engine`) | `candidate` | `partial` | `not-required` | `covered` | `missing` | `missing` | 2 | 3 |
| `P1` | **Supply-chain traceability and visibility** (`supply-chain-visibility`) | `candidate` | `partial` | `partial` | `covered` | `missing` | `missing` | 3 | 3 |
| `P2` | **External business API** (`external-business-api`) | `canonical` | `covered` | `covered` | `partial` | `covered` | `covered` | 3 | 1 |
| `P2` | **Internal domain event backbone** (`internal-domain-event-backbone`) | `canonical` | `covered` | `not-required` | `partial` | `not-required` | `covered` | 1 | 1 |

## P0 research queue

### External asynchronous integration (`external-async-integration`)

**Coverage:** `missing`  
**Contract:** `unresolved` - Unselected external event, webhook, subscription, or document-delivery contract

The boundary is explicit but the repository intentionally has not selected a public asynchronous contract. Internal JetStream topology is not acceptable evidence for a customer-facing event contract.

**Standards**: `missing` (useful)
- Gap: Compare event-envelope and contract-description standards without assuming either is the delivery mechanism.
- Target: CloudEvents
- Target: AsyncAPI
- Target: standards-based event subscription profiles used by logistics platforms

**Open Source**: `missing` (required)
- Gap: Capture at least two maintained implementations with explicit retry, replay, and dead-letter semantics.
- Target: webhook delivery engines with replay and signature semantics
- Target: durable subscription and pull-feed reference implementations

**Commercial**: `missing` (required)
- Gap: Capture product-specific public evidence for delivery guarantees, replay, subscription scope, and compatibility.
- Target: Manhattan Active event or webhook integration model
- Target: Blue Yonder integration event model
- Target: SAP EWM event and webhook surfaces
- Target: Logiwa outbound event model

**Executable**: `missing` (required)
- Gap: Compare duplicate delivery, replay windows, acknowledgement, rate limiting, and dead-letter ownership.
- Target: signed webhook lab
- Target: durable pull-feed lab

### Spotwo Edge control and synchronization (`edge-control-and-sync`)

**Coverage:** `partial`  
**Contract:** `unresolved` - Unselected command, configuration, synchronization, and health contract

The repository has useful edge-management references, but no selected Spotwo control-plane contract and no executable enrollment, desired/reported state, reconnect, or reconciliation lab.

**Standards**: `missing` (useful)
- Gap: Determine whether a standard materially helps enrollment, desired state, OTA, or credential rotation.
- Target: device-management and software-update standards relevant to managed industrial edge fleets

**Open Source**: `partial` (required)
- Gap: Compare desired/reported state, signed commands, OTA ownership, rollback, and local component supervision.
- Target: Mender
- Target: RAUC
- Target: Eclipse hawkBit

**Commercial**: `missing` (useful)
- Gap: Capture failure-domain and offline reconciliation patterns from managed fleets.
- Target: AWS IoT device-management architecture
- Target: Azure IoT device twins and edge management
- Target: industrial edge fleet managers used by automation vendors

**Executable**: `missing` (required)
- Gap: Exercise enrollment, credential rotation, stale desired state, offline execution evidence, and conflict resolution.
- Target: fake Edge enrollment and reconnect lab

### PLC and machine control (`plc-control`)

**Coverage:** `partial`  
**Contract:** `candidate` - Spotwo-owned PLC port with PLC4X, OPC UA, or vendor-native protocol adapters

The Spotwo-owned PLC port is defined and PLC4X is a concrete implementation candidate, but physical-controller compatibility, command safety, diagnostics, and OPC UA information-model tradeoffs are not yet measured.

**Standards**: `partial` (required)
- Gap: Separate protocol transport, machine state model, and security architecture instead of treating them as one standard.
- Target: ISA-95 equipment hierarchy mappings
- Target: PackML state model
- Target: IEC 62443 security zones and conduits

**Open Source**: `covered` (required)
- Gap: Add a strong OPC UA implementation reference for comparison with PLC4X.
- Target: open62541

**Commercial**: `missing` (useful)
- Gap: Capture public evidence for ownership between WMS/WES/WCS/MFC and PLC-level control.
- Target: SAP EWM MFS
- Target: Dematic controls
- Target: KNAPP KiSoft WCS
- Target: SSI SCHAEFER WAMAS MFS
- Target: Swisslog SynQ controls

**Executable**: `missing` (required)
- Gap: Test disconnect, timeout, reconnect, stale state, duplicate command, and physical-completion evidence.
- Target: Siemens S7 PLC4X lab
- Target: Modbus PLC4X lab
- Target: OPC UA adapter lab

### AMR and AGV fleet control (`robot-fleet-control`)

**Coverage:** `partial`  
**Contract:** `candidate` - Spotwo-owned robot adapter with VDA 5050 where supported and vendor-native adapters otherwise

VDA 5050, MassRobotics, openTCS, and Open-RMF give strong reference coverage, but the exact Spotwo Mission mapping and responsibility split with real fleet managers still need simulator and vendor evidence.

**Standards**: `partial` (required)
- Gap: Map command semantics separately from fleet visibility and capability reporting.

**Open Source**: `covered` (required)

**Commercial**: `missing` (useful)
- Gap: Capture public fleet-manager boundaries, mission ownership, traffic ownership, and interoperability support.
- Target: AutoStore
- Target: Exotec
- Target: Geek+
- Target: Locus Robotics
- Target: Ocado

**Executable**: `missing` (required)
- Gap: Prove duplicate dispatch, order revision, reconnect, error recovery, and capability negotiation.
- Target: VDA 5050 simulator lab
- Target: openTCS adapter lab

## Interpretation

`covered` means all required reference lenses are covered for the current architecture question. `partial` means relevant evidence exists but at least one required lens is still incomplete. `missing` means required research has no captured evidence yet. A research target is not evidence and must never be treated as a claim about a product, standard, or implementation.
