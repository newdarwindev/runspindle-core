# core specification

Status: planned contract, implementation and live acceptance not verified.

An isolated [contract-model proposal](contracts.md) now has a dependency-free
implementation candidate and synthetic tests. This is not a public wire protocol.
See [implementation evidence](../docs/IMPLEMENTATION_EVIDENCE.md) for checks actually
run and the remaining gaps.

## Responsibility

Rust coordination, Temporal integration, task identity and receipts.

## Shared identifiers

Correlate task, intent revision, selected host, native session, browser instance, lease epoch, trace/span, artifact digest and Slack destination without placing credentials or raw private content in public receipts.

## Required behavior

Preserve explicit authority and unknown outcomes. Distinguish request admission, execution, validation, acceptance and delivery. Exact state transitions belong in component-specific specs added beside this file.

## Acceptance

1. Unit/contract tests with synthetic data.
2. Isolated integration tests against pinned upstream dependencies.
3. Bounded live acceptance in an authorized target.
4. Failure, restart, uncertain-effect reconciliation and rollback evidence.

Isolated integration, live acceptance, restart and rollback gates have not run.
The synthetic component test status is recorded separately in the evidence document.
