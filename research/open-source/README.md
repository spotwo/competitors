# Open-source logistics architecture research

This directory tracks open-source projects, public specifications, protocols, and reference implementations that can inform or accelerate Spotwo.

The goal is not to accumulate dependencies. The goal is to separate four decisions for every upstream project:

1. **Adopt** - use it directly when it is mature and fits the boundary.
2. **Prototype** - prove integration or performance before any adoption decision.
3. **Study** - copy architectural ideas, domain vocabulary, contracts, or test strategy without taking a runtime dependency.
4. **Watch** - keep it on the radar because the category matters, but there is no immediate implementation need.

The canonical machine-readable inventory is [`catalog.yml`](./catalog.yml). Its shape is enforced by [`schema/open-source-project.schema.json`](../../schema/open-source-project.schema.json) and `scripts/validate_open_source_research.py`.

## Target architecture

```text
                         ERP / OMS / TMS
                              |
                  +-----------+-----------+
                  |                       |
             Spotwo WMS               External WMS
                  |                       |
                  +-----------+-----------+
                              |
                         Spotwo WES
                              |
                    domain orchestration
                              |
              +---------------+---------------+
              |               |               |
           WCS / MFC        Robots          Devices
              |               |               |
            PLC4X          VDA 5050       Spotwo Edge
              |           Open-RMF            |
              |            openTCS            |
              |               |               |
             PLC             AMR          Displays
                                           Printers
                                           Scales
                                           Pick/Put-to-Light
                                           DWS
```

Cross-cutting building blocks:

```text
Business events                NATS JetStream
Supply-chain interoperability  GS1 EPCIS
Industrial telemetry/state     MQTT + Sparkplug
Robot command interoperability VDA 5050
Robot fleet interoperability   MassRobotics / Open-RMF
Simulation                     Ocava architectural model
Optimization                   OR-Tools / Timefold
Geospatial indexing            H3
Road graph / routing           OSRM / Valhalla
Transport documents            eCMR
Air cargo interoperability     IATA ONE Record
Ocean interoperability         DCSA standards/APIs
```

## Architectural position

Spotwo should not assume it must replace SAP EWM, Manhattan, Odoo, ERPNext, or a custom enterprise WMS. A stronger platform boundary is the execution layer between warehouse intent and physical execution:

```text
SAP / Manhattan / Odoo / ERPNext / custom WMS
                     |
              Spotwo execution
                     |
     +---------------+----------------+
     |               |                |
  conveyors       AMR/AGV          devices
  sorters         robot fleets      printers
  AS/RS                            displays
  PLCs                             scales / DWS
                                   Pick/Put-to-Light
```

That keeps upstream ERP/WMS choices replaceable while allowing Spotwo to own orchestration, execution semantics, device abstraction, observability, simulation, and interoperability.

## Priority model

- **P0** - investigate now. Likely to influence a canonical Spotwo boundary or lab.
- **P1** - strong next-wave candidate.
- **P2** - useful reference or future integration surface.
- **P3** - watch only.

`relevance` is scored from 1 to 5:

- `5` - directly affects a core Spotwo architectural decision.
- `4` - strong building block or reference for an adjacent subsystem.
- `3` - useful domain or integration reference.
- `2` - peripheral.
- `1` - informational only.

The score is not a vendor endorsement and is not an adoption decision.

## Immediate P0 research set

The first deep-dive set is intentionally small even though the catalog is broad:

- **Ocava** - discrete-event simulation and the production/simulation clock pattern.
- **OpenWMS** - warehouse, transport-unit, movement, routing, and MFC domain modeling.
- **VDA 5050** - canonical AMR/AGV command interoperability boundary.
- **Open-RMF / openTCS** - heterogeneous fleet orchestration and transport control.
- **Apache PLC4X** - PLC protocol abstraction.
- **Eclipse Sparkplug / Tahu** - stateful industrial telemetry over MQTT.
- **GS1 EPCIS** - external supply-chain event semantics.
- **Google OR-Tools** - optimization baseline before writing custom solvers.

## Rules for adopting upstream code

Before promoting any project from `study` or `prototype` to `adopt`, verify at least:

- current maintenance status and release cadence;
- license and redistribution obligations;
- supported runtime/language and deployment footprint;
- protocol/version compatibility with target hardware;
- failure semantics, offline behavior, and observability;
- security model and update process;
- ability to replace the dependency behind a Spotwo-owned adapter or port.

Prefer a Spotwo-owned domain boundary with adapters around external projects. Standards such as EPCIS and VDA 5050 should normally be integration contracts, not the internal domain model.

## Research maintenance

Every record in `catalog.yml` must include:

- stable `id`;
- upstream organization and project name;
- category and architectural layers;
- source URL;
- Spotwo relevance score and priority;
- explicit disposition;
- direct-use, architecture-study, and prototype flags;
- rationale and concrete ideas to inspect.

Run all repository checks with:

```bash
bin/check
```

This research is an architecture and engineering inventory, not a legal or license audit.