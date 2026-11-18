# Artifact attestation diagnostic boundary

Core #18's optional authenticated-artifact wire decoder now discards parser
errors that can quote attacker-controlled member names or scalar values. It
returns the fixed `invalid artifact evidence encoding` error without retaining
the original error as a cause. This includes nested unknown fields, wrong typed
values and malformed JSON; display, alternate display and debug formatting must
not reveal the synthetic canary. Oversize, canonical-wire, validation, signature
and authorization checks retain their existing fail-closed behavior.

The regression exercises the actual Rust decoder, independently checks the error
chain has one fixed entry, verifies a valid canonical proof still round-trips,
and confirms no validation or acceptance row is created. The maintained mutation
suite now removes the mapping and requires that regression to fail behaviorally.

This is an ingress diagnostics fix, not a replacement of the authenticated
artifact model or its trust source. It adds no native producer custody, external
validator, credentials, delivery authority, live execution or production
qualification. Core #18 remains open; existing historical evidence and required
prepared-runner/aggregate gates remain separate and unchanged.
