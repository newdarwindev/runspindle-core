# runspindle-core

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
