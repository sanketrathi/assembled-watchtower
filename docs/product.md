# Product

## Who this is for

Watchtower is for support team leads. It helps them spot queue and agent issues
while the day is still in progress.

## The core loop

A team lead creates a rule, replays a stream of events, and sees alerts and
visible notification records.

The guided rule screen supports three rule types:

- **Queue SLA breach** — a queue waits longer than its SLA for a set time.
- **Adherence issue** — selected agents stay out of adherence for a set time.
- **Long-running call** — selected agents stay on one call for a set time.

Rules use explicit queue or agent IDs. In a production product, a team lead’s
agent list would come from the organisation directory. Group management is not
part of this demo.

## Alert behaviour

- A condition opens one alert for each affected queue or agent.
- A condition qualifies when it has been true for at least its configured time.
- More evidence for an open condition does not send another open notification.
- A recovery creates one recovery notification.
- A later recurrence can create a new alert.
- Notifications are stored as visible stub deliveries. They do not send Slack,
  email, or push messages.

## Demo rules

The supplied replay includes three active rules:

- Billing queue breaches its SLA for five minutes.
- Agents `a_19`, `a_88`, and `a_23` are out of adherence for ten minutes.
- Agent `a_31` stays on a call for 45 minutes.

The supplied fixture produces this notification order:

```text
09:35:00  Billing OPEN
09:45:00  a_19 OPEN
10:10:00  a_88 OPEN
10:10:30  a_19 RECOVERY
10:15:00  a_31 OPEN
10:15:00  Billing RECOVERY
10:25:00  a_31 RECOVERY
10:25:30  a_23 OPEN
```

## Not included

This demo does not include real delivery providers, login, tenant setup,
escalation, schedules, acknowledgments, or configurable groups.
