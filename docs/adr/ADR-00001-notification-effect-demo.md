# ADR 00001: A dedicated consumer creates the notification demo effect

Status: accepted · 2026-09-15

The direct in-memory effect is superseded by [ADR 00002](ADR-00002-confirmation-event-chain.md).

## Context

Observer cards establish record consumption, not business processing. The first
session needs a visible application reaction without adding an email provider or
confusing browser rendering with a Kafka consumer's work.

## Decision

A lessons-local NotificationRuntime owns one explicitly started KafkaConsumer and
a bounded in-memory inbox. A dedicated group, derived from the experiment prefix,
is independent of observer and A/B groups. It consumes the configured experiment
topic. Place order registers a correlation ID and publishes OrderPlaced through the
existing KafkaTemplate. Only correlated demo events from this backend session can
create a notification. The consumer records reception, performs a simulated 1.5 s
work delay, then generates and stores the confirmation text. Acknowledgment and
completion timestamps are independent. The effect is an actual local data change;
no external notification delivery is claimed.

The existing experiment sampler carries a nested notifications snapshot over the
same topic socket. This avoids a second transport and avoids competing retained
snapshot types on the shared channel. The demo shares experiment enablement and
topic configuration. Browser navigation only renders; commands own start/stop,
production, explicit topic observation, and stopped-inbox clearing.

## Consequences

The inbox retains at most 12 submitted orders, with the latest four completed
notifications displayed. Clearing requires a stopped service; it changes local
state, not Kafka records or offsets. Backend restart loses local effects and
correlations, while Kafka commits remain. Redelivery and durable idempotency are
outside this lesson's guarantee. No automatic retry is used for ambiguous sends.
Consumer operations stay on their owning thread except Kafka's thread-safe wakeup.
The service shuts down with the application. Secured environments must authorize
the derived notifications group separately; no credentials or ACL changes are made
by this feature.
