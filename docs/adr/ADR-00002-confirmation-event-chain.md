# ADR 00002: The notification result crosses a second Kafka topic

Status: accepted · 2026-09-15 · supersedes ADR 00001's direct in-memory effect.

## Decision

NotificationRuntime hosts two consumers on separate threads and in separate groups.
The confirmation service consumes the experiment topic, prepares a confirmation
and publishes OrderConfirmationPrepared to the configured confirmation topic.
The inbox consumer reads that output topic; only that read populates notification
text and completedAt in the bounded local inbox. This makes consume → process →
produce → consume visible, without requiring separate deployed applications.

Both events are keyed by order ID. The output has a stable eventId derived from
the input ID, causationId referencing that input, orderId, message, type and demo
marker. Both consumers validate/correlate session events. Publication metadata and
inbox-read metadata are recorded independently. The four stages can be observed
close together or in a different order; an acknowledgment is not receipt evidence.

Start/stop controls the whole chain. Placement is enabled only once both consumers
have assignments and resolved initial positions, avoiding missed first records with
latest. A failure stops the chain and leaves the input batch uncommitted if output
publication failed. Navigation never starts consumers. The existing experiment
snapshot on the input observation channel carries the entire chain state, so the
browser retains one socket. Topic setup remains explicit; no automatic creation.

## Limits

Input commit and output publication are separate operations, not a Kafka transaction.
A crash after publication can produce duplicate output records on explicit restart.
The stable event ID is useful for correlation, not a durable deduplication guarantee.
The inbox and at most 12 correlations are session-local; restart/clear loses them
while Kafka records and commits remain. Only this session's registered orders are
eligible. This is a bounded teaching chain, not an arbitrary replay service.

Configuration adds demo.notifications.confirmation-topic (DEMO_CONFIRMATION_TOPIC),
default kafka-demo-confirmations. It must be distinct from the input and present in
demo.topics. The new inbox group is <experiment-prefix>-notification-inbox; the
service retains <experiment-prefix>-notifications. Secured deployments must grant
the corresponding topic/group access. Tests use a real embedded broker to audit the
output payload/key/correlation and compare publication and inbox-read metadata.
