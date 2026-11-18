# Wire parsing diagnostics

Runtime envelopes, scoped admissions and intent/batch ingress now discard original
JSON/type-deserializer errors. Unknown field names, unexpected string values and
nested keys are caller-controlled and must not become returned error text or
chained causes. The fixed categories are `invalid wire JSON` and
`invalid wire structure`; existing semantic validation categories remain unchanged.

The common typed boundary also covers direct public `Envelope.message()` mapping,
not just byte decoding. Canonicalization, duplicate-key rejection, byte limits,
authority verification and typed schemas are unchanged. This is a diagnostic
boundary only: successful parsing grants no authority and establishes no live
transport/native qualification. Durable-store and synthetic script diagnostics
are outside this wire-ingress change.

Regression cases use synthetic canaries across every envelope kind, binding and
payload, malformed primitive types and the scoped/intent entrypoints. They check
both displayed/debug errors and the complete one-element cause chain. Existing
valid conformance vectors must continue mapping/roundtripping unchanged.
