# ADR-0003: Separate physical structure from addressable storage location

- Status: Proposed
- Date: 2026-08-18
- Scope: Domain model / terminology

## Context

Warehouse products do not expose one universal physical hierarchy. SAP EWM models storage type, storage section, storage bin and activity area; a storage-bin coordinate can encode aisle, stack and level. Oracle WMS uses a generic Location concept with typed locations such as Active, Dock, Yard, Packing Station, Drop, QC and Reserve. GS1 location standards are broader still and treat a dock door as a fixed physical location.

A hard-coded `ROW -> RACK -> LEVEL -> BIN` hierarchy would therefore mix physical equipment, coordinate components and logical addressable locations.

## Proposed decision

Use `Location` as Spotwo's canonical addressable-place abstraction.

```text
Site
  Building
    Warehouse
      Zone / Storage Area
        Location

Optional physical/coordinate dimensions:
Aisle
Bay / Stack
Level
Rack
Bin (Location subtype / UI vocabulary)
Dock Door
Station
```

`Rack` is physical storage equipment that can host locations. `Bin` is a leaf/storage-location term where appropriate, not a mandatory physical container.

Customer-specific hierarchies such as `ROW -> RACK -> LEVEL -> BIN` remain supported as configured display/address structures.

## Consequences

- The core model can represent racking, floor storage, docks, packing stations and automation cells.
- UI can still present a simple ROW/RACK/LEVEL/BIN hierarchy where it matches the facility.
- Integrations map vendor concepts to Spotwo Location plus optional structural dimensions instead of changing the core schema.

## Evidence

- `sap-ewm-storage-bin-2026`
- `sap-ewm-warehouse-structure-2026`
- `oracle-wms-location-master-2026`
- `gs1-gln-location-model`
