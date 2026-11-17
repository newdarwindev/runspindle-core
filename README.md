# RUNSPINDLE CORE

## One authorized job. One durable identity. Evidence that survives interruption.

**CORE — DURABLE INTENT AND EFFECT COORDINATION** binds a task to its operation,
selected host, workspace and native-session identity. It distinguishes a repeated
request from a conflicting revision, checks scoped authority and preserves
uncertainty instead of turning a missing reply into success.

[Temporal](https://temporal.io/) provides durable workflow progression; SQLite
provides the implemented local persistent records. **CORE** supplies the admission
rules, immutable bindings and effect semantics around those foundations. Recovery
must continue the same authorized work, not silently dispatch a replacement on
another machine.

The bounded F02 acceptance is complete: [Core #2](https://github.com/newdarwindev/runspindle-core/issues/2)
is closed following [interruption/upgrade qualification](docs/AUTHENTICATED_CHECKPOINTS.md)
and [exact-component composition](https://github.com/newdarwindev/runspindle/pull/76).
The evidence uses real Temporal and SQLite with controlled issuers and scripted
peers. It includes 44 checkpoint/revocation cases and six pinned old-binary upgrades,
not a production-system certification.

*UPCOMING — qualify production issuer custody, actual native/provider boundaries,
and full live journeys. The tested upgrade pairs do not prove arbitrary version
compatibility, and process-kill recovery does not prove power-loss durability.*

[![Conceptual identity and evidence chain, gallery page 18](https://newdarwindev.github.io/runspindle_for_bloomberg/gallery/18.webp)](https://newdarwindev.github.io/runspindle_for_bloomberg/assets/Runspindle_Gallery_Index.html#page-18)

**Concept illustration, not a running Console screenshot.** The gallery connects
authority, exact artifacts, delivery receipts and evaluation into one inspectable
record chain. [Slide/source index](https://newdarwindev.github.io/runspindle_for_bloomberg/assets/Runspindle_Gallery_Index.html#page-18)
· [Research proposal](https://github.com/newdarwindev/runspindle_for_bloomberg/blob/main/assets/runspindle-paper.pdf)
· [Product concept slides](https://github.com/newdarwindev/runspindle_for_bloomberg/blob/main/assets/runspindle_bloomberg_product.pdf).
These linked materials contain proposed workflows and synthetic examples, not
measured savings, live trading or Bloomberg endorsement. See the
[source asset guide](https://github.com/newdarwindev/runspindle_for_bloomberg/blob/main/ASSETS.md).

### Follow the implementation

- [Scoped authority](docs/FOUNDATION_AUTHORITY.md) and [signed assertions](docs/VERIFIED_AUTHORITY.md).
- [Tether intent bridge](docs/INTENT_BRIDGE.md): semantic revisions and durable recovery.
- [Host runtime](docs/HOST_RUNTIME.md), [artifact store](specs/durable-artifacts.md), and [delivery/outbox semantics](specs/durable-delivery.md).
- [Current source](src/), [tests](tests/) and [machine-readable status](STATUS.json).

## Development and qualification boundaries

Rust coordination, Temporal integration, task identity and receipts.

**Status: bounded Temporal development runtime; integrated/live acceptance remains blocked.**

The default dependency-free models retain their process-local limits. The opt-in
`temporal-runtime` feature adds a real Rust Temporal client/worker, bounded durable
admission registry and operation workflows with scripted synthetic activity ports.
Fresh-process worker/server recovery and real history replay are tested against a
pinned disposable development server. This supplies no production/native/browser/
provider/delivery authority or deployment. Read [runtime semantics](specs/runtime.md)
and [preparation/checks](docs/TEMPORAL_RUNTIME.md) for exact evidence and remaining gaps.
The fixture schema remains a separate test format.

This repository is one boundary of [Runspindle](https://github.com/newdarwindev/runspindle). Repository boundaries do not require one microservice each.

- Read [specifications](specs/README.md), [agent guidance](AGENTS.md), [provenance](provenance.json), [status](STATUS.json) and [legal status](LEGAL.md).
- Run `python3 tools/check_scaffold.py` for structural validation only.
- Run `cargo test --offline --all-targets` for synthetic component tests when a qualified Rust toolchain is available. Tests are not integration or deployment acceptance.
- Run `python3 tools/check_shared_fixtures.py` for the [pinned synthetic fixture consumer](docs/SHARED_FIXTURES.md); actual runtime adapter conformance remains blocked.
- Operational entrypoints and acceptance gates deliberately fail closed until implemented.
- GitHub Actions use isolated **self-hosted local runners only**. No GitHub-hosted fallback. Runner capacity is not established by publishing workflows.
- `rust-ci.yml` is configured to run the structural check, Rust formatting, offline tests and offline clippy using a pre-provisioned toolchain. A missing toolchain fails the job; nothing is installed or silently skipped. These are configured checks, not evidence that CI ran.
- No credentials, real sessions, messages, datasets, browser profiles or live host inventory belong here.

The model has no host-observation freshness proof, bounded ledger retention or
cross-object delivery deduplication. See the contract proposal before building any
runtime adapter around it.

No old repository is retired by this scaffold. Migration and archival are separate verified gates.

## Implemented validation

Run `python3 tools/validate.py` for all implemented structural, language, fixture, artifact and mutation checks in this repository. The same entrypoint runs on trusted `main` and `ci/*` pushes and manual qualification using disposable Strix x64 guests. Logs include the checkout SHA, image/tool identities, command results and applicable artifact checksums. Unimplemented builds and product acceptance remain explicit in `STATUS.json`; blocked acceptance workflows retain their failing conclusions. ARM64 and native macOS qualification remain pending.

## Desired technical direction

Durable intent and effect coordination: this component contributes to turning authorized one-off work into
repeatable, maintained cross-application workflows. Scoped access, managed
sessions, explicit human exceptions and evaluated DSPy candidates are intended
to compose into that capability; no automatic improvement or live integration
is implied.

Read [the component-specific technical intent](specs/desired-intent.md) for its
interfaces, privacy boundaries, failure cases and acceptance obligations. Current
implementation and evidence limits above remain unchanged.

The optional [real host library lane](docs/HOST_RUNTIME.md) connects Temporal
activities to the pinned durable host adapter, with a separate scripted native
peer ledger. Native app-server and live acceptance remain unqualified.

The [durable artifact store](specs/durable-artifacts.md) adds immutable byte storage,
validation and default-deny authorized acceptance. Delivery and live gates remain separate.

The [durable delivery mapping](specs/durable-delivery.md) adds a single-send outbox and
real Temporal H07 workflow with a scripted destination ledger. Actual Tether and
full-system/live acceptance remain blocked.

The optional `verified-authority` feature adds an offline signed-assertion
admission lane; see [scope and reproducible checks](docs/VERIFIED_AUTHORITY.md).
It does not enable production authentication or change the historical development
workflows. Runtime envelopes are separate from existing library/history formats.
