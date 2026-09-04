# Design

## One clear path

Watchtower uses one deterministic replay path for the demo:

1. Read each JSONL line in physical order.
2. Store the raw occurrence in PostgreSQL.
3. Keep the newest queue, agent-state, or adherence view for each subject.
4. Move a nondecreasing logical clock forward.
5. Fire due rule timers before applying evidence at that clock time.
6. Turn condition changes into alerts and notification intents.
7. Store one visible stub delivery for each intent.

The event timestamp is evidence time. A late event is retained, but it cannot
move the live view or logical clock backwards.

## Data choices

An upstream `event_id` is not enough to identify a source occurrence. The demo
uses the replay stream and physical line number. This keeps two different lines
with a repeated source ID.

Queue snapshots, agent state, and adherence are separate views. Event
`queue_ids` are evidence only. They do not define team membership.

## Delivery

The delivery record is a stub. It makes the result visible in the dashboard and
in `GET /api/notifications`. Real provider integration is outside the demo.

## HTTP surface

- `GET /` — team-lead dashboard.
- `GET /rules/new` — guided rule form.
- `POST /rules` — save a rule or preview it against the fixture.
- `GET /api/alerts` — stored alert records.
- `GET /api/notifications` — stored notification records.
- `GET /healthz` — database health check.

## Limits

Replay is single-process and deterministic. It is designed for a small,
inspectable demo. A larger deployment would add partitioned workers, recovery
leases, and provider-specific delivery workers.
