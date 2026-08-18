# ADR-0004: Distinguish internal handling-unit identity from GS1 SSCC

- Status: Proposed
- Date: 2026-08-18
- Scope: Domain model / identification

## Context

GS1 defines SSCC as the identification key for a logistic unit and requires it as the unique identifier on a GS1 Logistic Label. WMS products also use internal container identities. Oracle WMS, for example, uses LPNs that can represent a box or an entire pallet.

These concepts overlap operationally but are not equivalent.

## Proposed decision

```text
HandlingUnit
  id                 # Spotwo internal immutable identifier
  code               # human/scannable internal code when required
  type               # pallet, case, tote, parcel, ...
  parent_id           # optional nesting
  lpn                 # optional vendor/customer WMS license plate
  sscc                # optional GS1 logistic-unit identifier
```

Do not call every internal handling-unit identifier an SSCC.
Do not make `LPN` the canonical entity name because LPN semantics vary by WMS.
Use `Handling Unit` as the internal domain concept and preserve LPN and SSCC as explicit identifiers.

## Consequences

- Internal tote and pallet tracking works without requiring GS1 allocation.
- External logistics labels can carry standards-compliant SSCCs.
- Imported WMS LPNs retain their source meaning.
- A handling unit may also be a logistic unit when the external supply-chain process requires it.

## Evidence

- `gs1-logistic-label-guideline`
- `oracle-wms-lpn-uom-2026`
