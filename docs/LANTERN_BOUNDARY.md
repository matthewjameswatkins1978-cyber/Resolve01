# Lantern Keeper boundary

Lantern Keeper owns historical truth, evidence, history, and provenance.
Resolve may receive context from Lantern and may return meaningful completed
history, but Resolve does not own Lantern's database schema or duplicate
Lantern's memory model.

Resolve owns only live operational coordination: goals, commitments, worker
claims and leases, guards, scope locks, waiting/recovery state, and its own
immutable work-event log.

S0 establishes and documents this ownership boundary. No Lantern dependency,
database table, or integration is added to Resolve01.
