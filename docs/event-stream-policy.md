# Event replay rules

Watchtower reads the supplied JSONL file in physical line order. It accepts three
event types:

- `queue_snapshot`
- `agent_state_change`
- `adherence_check`

Each event has an `event_id`, a timestamp, and a type. The decoder rejects
unknown fields and invalid values.

## Identity and order

A source `event_id` is not a unique occurrence key. The fixture contains the
same source ID on two different lines. Watchtower keeps both records.

For replay, a record is identified by the input stream and its one-based line
number. The file is never sorted before processing.

## Time

`ts` is the event's effective time. Watchtower keeps a nondecreasing logical
clock. Before applying an event, it fires due duration timers through that
clock time.

A late record remains stored evidence. It cannot move a live queue, agent, or
adherence view backwards. It also cannot rewrite an alert or notification that
was already emitted.

A condition that lasts exactly its configured duration qualifies. At the end of
the fixture, Watchtower fires timers due at or before the latest observed time.
It does not use wall-clock time to force more alerts.

## Source views

Queue snapshots update queue state. Agent state changes update agent state.
Adherence checks update adherence state. These are separate views.

`queue_ids` in an agent event are source evidence only. They do not define team
membership or change a rule's targets.

For adherence, `in_violation` is the source of truth. When
`violation_started_at` is present, it supplies the known start time. When it is
missing, duration tracking starts at the first known true check.

## Fixture behaviour

The fixture includes a late VIP snapshot, a repeated source event ID, adherence
checks with and without an onset time, and conflicting call-duration evidence.
The replay keeps that evidence while preserving the current operational view.
