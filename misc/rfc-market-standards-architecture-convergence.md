# RFC: Market, standards, and architecture convergence strategy

Status: **Deferred / strategic direction captured**

This RFC records a long-term research and architecture strategy for Spotwo. It is not an implementation commitment and does not select technologies by itself. Its purpose is to preserve a method for discovering the strongest possible Spotwo architecture by converging real-world product evidence with global standards.

## Thesis

Spotwo should not be designed from a blank sheet and should not copy one incumbent product.

The target approach is to research the market from two directions in parallel:

1. **Existing systems and implementations** - open-source and proprietary products, their architecture, entities, workflows, APIs, infrastructure, operating models, strengths, weaknesses, and missing capabilities.
2. **Standards and industry models** - GS1, EPCIS, ISA-95, ISA-88, OPC UA, VDA 5050, MassRobotics interoperability, Sparkplug, PackML, ISO/IEC and other relevant logistics, automation, robotics, identification, safety, security, and data-exchange standards.

These two research streams should converge into a Spotwo architecture that is standards-compatible where standards are useful, informed by proven systems where implementation experience matters, and intentionally better where both standards and current products leave gaps.

The objective is not a theoretically universal system. Fully universal systems do not exist. The objective is a highly adaptive, modular, composable platform that can cover a very large part of logistics and intralogistics without collapsing into one giant inflexible product.

## Core research question

For every important capability or architecture boundary, answer:

> What do mature systems already do, what do standards require or recommend, where do they agree, where do they disagree, what is missing, and what should Spotwo make canonical?

## Ten strategic research streams

### 1. Reference coverage gaps

Audit the complete Spotwo architecture and identify layers or capabilities for which we do not yet have strong references.

Examples to investigate include industrial connectivity, OPC UA/open62541, ROS 2/DDS, simulation, PackML, ISA-88/ISA-95 implementations, IEC 62443 security, conveyor and sortation control, barcode and dimensioning systems, print-and-apply, yard management, dock scheduling, labor management, carrier integration, and last-mile execution.

The goal is not to collect projects endlessly. The goal is to know where our current knowledge has blind spots.

### 2. Commercial reference architecture

Research proprietary systems with the same discipline currently applied to open source.

Candidate references include SAP EWM/MFS, Manhattan, Blue Yonder, Körber, Dematic, KNAPP, SSI Schäfer, Swisslog, AutoStore, Exotec, Geek+, Locus, Ocado and other significant WMS/WES/WCS/MFC/robotics vendors.

For each, capture where possible:

- product and subsystem boundaries;
- architecture layers;
- ownership of planning versus execution;
- integration contracts;
- canonical entities and naming;
- workflow and state models;
- deployment and infrastructure model;
- extensibility model;
- observed strengths;
- observed weaknesses;
- capabilities they do not cover or cover poorly.

The output should become a machine-readable proprietary-system reference matrix rather than a prose-only competitor list.

### 3. Standards map

Maintain an explicit map of relevant standards and specifications, not merely a list of standards names.

For every standard, record:

- the problem it solves;
- the architecture layer where it belongs;
- the entities and semantics it defines;
- whether it is normative, de facto, regional, vendor-neutral, or implementation-specific;
- which Spotwo boundaries should expose or consume it;
- where Spotwo should deliberately not use it;
- version and evidence references;
- mappings to Spotwo canonical concepts.

Examples include GS1/EPCIS, ISA-95, ISA-88, OPC UA, Sparkplug, VDA 5050, MassRobotics AMR interoperability, PackML, eCMR, DCSA and IATA ONE Record.

### 4. Architecture boundary map

This is a priority artifact.

Create a canonical map of every major Spotwo boundary, for example:

```text
ERP / OMS / TMS / external WMS
              |
              v
        Spotwo intent layer
              |
              v
          Spotwo WES
              |
              v
         WCS / MFC
              |
              v
   Robot / PLC / Device adapters
              |
              v
         Spotwo Edge
              |
              v
      PLC / Robot / Device
              |
              v
        Physical world
```

For every arrow, decide which contract is canonical and why.

Candidate mechanisms may include REST/OpenAPI, asynchronous domain events, NATS, MQTT/Sparkplug, OPC UA, VDA 5050, MassRobotics messages, industrial field protocols, file/document exchange, or vendor-specific adapters.

The boundary map must also define:

- command direction;
- event direction;
- ownership of state;
- source of truth;
- retry/idempotency expectations;
- versioning;
- offline behavior;
- security/trust boundary;
- simulation boundary;
- external-standard mappings.

The purpose is to prevent Spotwo from becoming a set of accidental point-to-point integrations.

### 5. Build vs buy vs adapt matrix

For every capability, make an explicit strategic classification such as:

- `build` - core Spotwo capability;
- `embed` - use an existing library/runtime inside Spotwo;
- `adapter` - integrate an external product or customer system;
- `external-standard` - support a standard without owning the implementation behind it;
- `partner` - leave implementation to hardware/system partners;
- `not-our-problem` - intentionally outside Spotwo core.

Examples of hypotheses to test:

```text
OR-Tools          embed / prototype
VDA 5050          external-standard
SAP EWM           adapter
PLC4X             embed or adapter foundation
PLC firmware      partner / not Spotwo core
```

The classification should be evidence-backed and revisable.

### 6. Simulation as a first-class architecture concern

Investigate whether Spotwo should make simulation a structural property rather than a separate test tool.

A strong target model would allow the same canonical execution concepts such as Task, Mission, Move, Device, Resource and Material Flow to execute against either:

```text
real executor
```

or

```text
simulation executor
```

without rewriting business semantics.

Research Ocava, digital-twin systems, robotics simulators, discrete-event simulation frameworks and commercial warehouse simulation approaches. Determine what needs to be abstracted early so simulation remains possible later.

### 7. Canonical object model convergence

Continuously compare Spotwo ontology against entities used by standards and mature systems.

At minimum, study and map concepts such as:

```text
Item
SKU
Inventory
Inventory Position
Handling Unit
Container
Transport Unit
Load Carrier
Location
Zone
Bin
Resource
Device
Machine
Robot
Task
Mission
Move
Transport Order
Shipment
Load
Route
Stop
Order
Work
Event
```

For each concept capture aliases, semantic differences, lifecycle, identity, ownership, relationships, and external mappings.

Do not rename Spotwo concepts merely because one vendor uses a term. Use evidence from multiple products and standards to decide the canonical model.

### 8. Labs backlog with decision-oriented acceptance criteria

Maintain a machine-readable backlog of architecture labs.

A lab must not exist merely to try a technology. Every lab should define:

- hypothesis;
- architecture question;
- input/reference systems;
- environment;
- test scenario;
- acceptance criteria;
- measurements;
- failure conditions;
- output decision;
- follow-up decision owner.

Example:

> Determine whether Apache PLC4X can serve as the canonical Spotwo PLC adapter layer across the protocols and operational constraints we actually need.

This is stronger than a generic "try PLC4X" task.

### 9. Negative references and rejected patterns

Record what Spotwo intentionally does not copy.

A useful research repository must preserve failed options and architectural anti-patterns, not only attractive references.

Examples:

- archived or abandoned dependencies;
- incorrect abstraction boundaries;
- overly heavy edge runtimes;
- vendor-locking APIs;
- duplicated state ownership;
- non-idempotent integration patterns;
- weak event/versioning models;
- concepts that work for one vertical but fail as general primitives.

Use explicit `reject` or `do-not-copy` decisions with evidence and rationale so future agents do not repeat the same investigation.

### 10. Reference watch and research freshness

Once the catalogs are mature enough to justify automation, periodically monitor important references for meaningful changes such as:

- new releases;
- archive/deprecation status;
- license changes;
- specification revisions;
- major architecture changes;
- new interoperability standards;
- important new production adopters;
- newly open-sourced components;
- major security or maintenance concerns.

This should be a later step, after evidence-backed records and freshness semantics exist. Automation should refresh knowledge, not create noisy dependency churn.

## Recommended execution order

The ten research streams above are not intended to run as ten independent projects at once. A practical default sequence is:

```text
1. Architecture Boundary Map
        ↓
2. Reference Coverage Gaps
        ↓
3. Commercial Systems + Standards
        ↓
4. Canonical Object Model Convergence
        ↓
5. Build / Embed / Adapter Matrix
        ↓
6. Decision-Oriented Labs
        ↓
7. Evidence Enrichment + Continuous Watch
```

This order is a recommendation, not a hard dependency graph. Research may move backward or run selected slices in parallel when evidence requires it.

### Why the Architecture Boundary Map comes first

The boundary map creates the skeleton onto which later evidence can attach. Without it, research can discover technologies and standards without answering which Spotwo boundary they are supposed to serve.

For example, the useful question is not simply whether Spotwo should use OPC UA, Sparkplug, VDA 5050, PLC4X, or EPCIS. The useful questions are:

- which architecture boundary each technology or standard belongs to;
- whether it is a canonical internal contract, an external interoperability contract, an adapter mechanism, or only a reference;
- who owns state on each side of that boundary;
- which commands, events, retries, security rules, offline semantics, and versioning rules cross it.

This prevents technology selection from driving the domain model accidentally.

### Why coverage gaps come second

Once the skeleton exists, audit each boundary and capability for missing evidence. This produces a bounded research backlog instead of an endless list of interesting technologies.

### Why commercial systems and standards come together

Study mature proprietary implementations and standards against the same boundaries. Proprietary systems show how real operations have been implemented under production constraints. Standards show where interoperability and shared semantics already exist. Their agreement, disagreement, and gaps become evidence for Spotwo decisions.

### Why canonical objects follow that research

Only after comparing multiple systems and standards should Spotwo converge entity names, identities, lifecycle semantics, ownership, and mappings. This reduces the risk of copying one vendor's vocabulary or forcing an external standard directly into the internal ontology.

### Why ownership decisions come after semantics

Once boundaries and objects are clear, classify each capability as `build`, `embed`, `adapter`, `external-standard`, `partner`, or `not-our-problem`. This keeps build-vs-buy decisions tied to the architecture rather than vendor popularity.

### Why labs come after the candidate architecture

Labs should answer unresolved architecture questions, not create architecture by accident. Each lab should therefore test a specific candidate boundary, dependency, protocol, solver, or execution model and produce a decision-oriented result.

### Why evidence enrichment and watch come last

Deep evidence enrichment and continuous freshness monitoring become most valuable after the important boundaries, capabilities, and candidate decisions are known. At that point automation can focus on references that actually influence Spotwo instead of maintaining a large undifferentiated technology catalog.

## Market research scope

The long-term market pass should deliberately include both **open-source** and **closed/proprietary** solutions.

For each relevant product or project, investigate not only features but its internal model as far as public evidence permits:

- what problem it owns;
- what it delegates to another system;
- architecture and subsystem decomposition;
- APIs, events and protocols;
- names of entities and domain concepts;
- data ownership and state transitions;
- code organization and extension mechanisms when visible;
- infrastructure and deployment model;
- edge/offline architecture;
- observability and operations;
- security boundaries;
- integration strategy;
- strengths;
- weaknesses;
- missing capabilities;
- historical architectural mistakes or migrations when evidence exists.

The purpose is to learn from systems that have already spent years solving the same classes of problems.

## Standards research scope

In parallel, continue expanding the standards corpus.

Standards must be treated as architecture inputs, not as decoration and not as unquestionable internal models.

For each standard ask:

1. What interoperability problem was it designed to solve?
2. Which participants and boundaries does it assume?
3. Which objects and semantics are reusable internally?
4. Which parts should remain external mappings only?
5. What extensions or profiles exist?
6. Which mature vendors actually implement it?
7. What happens when the standard is insufficient for Spotwo's use case?

The goal is to make Spotwo **standards-compatible and standards-relatable**, while retaining a clean internal model that is not distorted by every external specification.

## Convergence principle

The intended research process is:

```text
Existing open-source systems -----------\
                                         \
Existing proprietary systems ------------> evidence
                                           |
Operational practices -------------------->|
                                           v
                                  Spotwo canonical model
                                           ^
                                           |
Global standards ------------------------>|
                                           |
Research labs --------------------------->|
                                           |
Observed gaps / failures ---------------->|
```

We should converge in the middle.

Do not blindly copy products.
Do not blindly implement standards.
Do not invent concepts that the industry has already solved well.
Do not inherit historical vendor limitations merely because they are common.

Instead:

- reuse proven ideas;
- map to standards where interoperability matters;
- preserve adapters where external semantics differ;
- improve weak abstractions;
- fill missing capabilities;
- keep modules replaceable;
- keep boundaries explicit;
- keep evidence behind decisions.

## Desired Spotwo outcome

The target is a platform that is:

- modular;
- adaptive;
- composable;
- evidence-driven;
- standards-compatible;
- vendor-neutral where practical;
- capable of integrating proprietary and open systems;
- simulation-friendly;
- edge-aware;
- automation/robotics-aware;
- usable from manual warehouses through highly automated facilities;
- explicit about what is core and what is an adapter;
- capable of evolving without rewriting the whole ontology or execution model.

"Universal" is an aspiration boundary, not a promise. The architecture should maximize useful generality while keeping domain-specific behavior in capabilities, policies, adapters, profiles and modules instead of weakening the canonical core.

## Strategic success criterion

A future Spotwo architecture decision should be explainable in a chain like:

```text
market evidence
+ open-source implementation evidence
+ proprietary-system evidence
+ standards evidence
+ lab evidence
+ Spotwo constraints
= canonical Spotwo decision
```

If an important decision cannot show that chain, it should be treated as insufficiently researched or explicitly documented as an intentional experiment.

## Relationship to the evidence-backed open-source RFC

`misc/rfc-evidence-backed-open-source-research.md` is one implementation slice of this broader strategy. It covers how to deepen the open-source side of the evidence base.

This RFC expands the scope to include proprietary systems, standards, architecture boundaries, canonical object convergence, labs, negative references, and long-term research freshness.

## Non-goals

This RFC does not:

- claim that one architecture can fit every logistics company without adaptation;
- require Spotwo to implement every standard internally;
- require Spotwo to replace ERP, OMS, TMS, WMS, PLC, robot fleet manager or every other system;
- adopt any technology named above;
- start the proposed labs;
- mandate compatibility with a standard before its relevance is proven;
- treat incumbent vendor behavior as automatically correct;
- optimize for feature count over coherent boundaries.

## Future prompt direction

A later agent prompt derived from this RFC should deeply research one architecture slice at a time, using primary evidence, and produce machine-readable mappings across:

```text
capability
Spotwo boundary
open-source references
proprietary references
standards
canonical entities
commands/events
known weaknesses
missing capabilities
candidate architecture
lab needed
final disposition
```

The highest-priority future slice should be the **Architecture Boundary Map**, because it provides the skeleton onto which the remaining product, standards, ontology, protocol, and integration research can attach.
