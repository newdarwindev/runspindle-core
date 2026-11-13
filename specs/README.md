# core specification

Status: bounded Temporal development implementation; integrated/live acceptance not verified.

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

The bounded real SDK/Temporal lane now separately exercises development persistence,
fresh-process recovery and same-version history replay; see [runtime mapping](runtime.md).
Native/browser/provider/delivery integration, live acceptance and production rollback remain blocked.
The synthetic component test status is recorded separately in the evidence document.

## Engineering direction

[Desired intent](desired-intent.md) connects this component to scoped access,
managed sessions, explicit human exceptions and evaluated maintained workflows.
It adds design and verification obligations without changing runtime schemas,
existing acceptance results or operational permissions.
