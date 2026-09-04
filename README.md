# Watchtower

Watchtower helps support team leads notice queue and agent problems during a
busy day. It turns a stream of contact-center events into clear alerts and
visible notification records.

## What you can do

- Create a rule from one of three guided templates:
  - queue SLA breach;
  - agent adherence issue; or
  - long-running call.
- Replay the supplied morning of events in a fixed order.
- See open alerts and delivered stub notifications in the dashboard.
- Inspect alerts and notifications as JSON.

The demo includes three sample rules for Billing SLA, adherence, and long calls.

## Run the demo

You need Docker and Docker Compose.

```sh
docker compose down -v
docker compose up -d --build
docker compose --profile demo run --rm --build replay
open http://localhost:8080
```

On Linux, open `http://localhost:8080` in your browser instead.

Useful inspection endpoints:

```sh
curl http://localhost:8080/healthz
curl http://localhost:8080/api/alerts
curl http://localhost:8080/api/notifications
```

To start the fixture replay again with a clean database:

```sh
docker compose down -v
```

## How it works

The replay reads [`data/events.jsonl`](data/events.jsonl) line by line. Physical
line order is preserved. Event time drives a logical clock, so late records do
not move live state backwards. A continuing condition opens one alert. Its
recovery closes that alert. The stub delivery is stored in PostgreSQL and shown
in the dashboard.

The fixture has 96 events across three queues and eight agents. Its expected
notification order is recorded in
[`data/expected-notifications.jsonl`](data/expected-notifications.jsonl).

## Scope

Watchtower is built for team leads. It uses explicit queue and agent selection.
A production version would map “my agents” to team membership managed elsewhere.

The demo does not include real Slack, email, or push delivery. It also does not
include login, tenant setup, escalation rules, schedules, or configurable
groups.

## Design notes

- [Product behavior](docs/product.md)
- [Event stream rules](docs/event-stream-policy.md)
- [Design](docs/design.md)

## Development checks

```sh
go test ./...
go vet ./...
make fmt-check
```
