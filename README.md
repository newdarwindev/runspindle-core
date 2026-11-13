# runspindle-core

Rust coordination, Temporal integration, task identity and receipts.

**Status: proposed in-memory contract slice, not a deployed or production-ready implementation.**

The dependency-free Rust library models artifact evidence transitions, delivery
uncertainty, request deduplication and explicit host pins. These are proposed model
types, not an agreed cross-component protocol. It performs no I/O, authenticates no
evidence and cannot activate native sessions or Temporal. See the
[contract proposal](specs/contracts.md) and [evidence and gaps](docs/IMPLEMENTATION_EVIDENCE.md).

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
