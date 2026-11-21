# Passive readback of canonical Browser control

`BrowserRegistry::passive_control_status(selection, authority)` reads retained
control metadata for one exact application selection. It checks the separately
supplied `PassiveAuthority` before opening the read transaction and again
before releasing a result. The authority must bind and verify the entire
selection, including intent, revision, operation, and epoch. An input or
transfer grant provides no passive authority through this interface.
[H09](https://github.com/newdarwindev/runspindle/issues/16),
[external control ownership](https://github.com/newdarwindev/runspindle-browser/issues/61).

The transaction validates the existing control schema and history, then selects
the head for the requested browser. Its physical scope must match exactly,
including the account, principal, native session, tab, frame and both origins.
A healthy store without a head returns `head: None`. A missing schema, invalid
history, or scope mismatch returns an error. The existing BrowserRegistry
remains the sole owner of retained transitions.

Each result echoes the complete admitted selection. A response for a previous
selection can therefore be identified after a task switch. The head has its own
control epoch and generation. A caller watching the same selection and store
can use those counters to discard reordered responses. Within that same store,
an absent response predates any retained head. The returned head can have a
more recent epoch than the selection supplied by the caller.

| Retained state | Meaning |
| --- | --- |
| `Pending` | The reserved head has an unknown outcome. |
| `Confirmed(holder)` | Core retained confirmation for this holder. |
| `Rejected` | Core retained a rejection for this head. |
| `Conflict` | Later evidence disagreed with the retained observation. |

Pending, rejected and conflicted heads expose no holder. The response carries
no physical timestamp or liveness claim. A fresh physical observation must
come separately from an authenticated Browser observation. The passive method
takes no transport or input peer, writes no rows, and never grants a lease or
permission to control the browser.

## Tests

```sh
cargo test --offline --locked --features artifact-store --test browser_control passive_control
cargo test --offline --locked --features artifact-store --test browser_control
```

The tests use a disposable SQLite registry and scripted fencing and readback
responses. They verify selection and scope checks, grant revocation before
return, absent and pending state, confirmed head counters, and refusal to
expose the holder for conflicted or rejected heads. A fresh test process reads
the same confirmed head, and the parent compares the database bytes before and
after. Removing the final passive authority check makes the revocation case
fail. These component tests leave authenticated application wiring, desktop
streaming, and H09 isolated composition qualification open.
