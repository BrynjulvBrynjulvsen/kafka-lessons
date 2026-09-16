# Architecture decisions

The historical records and current shared boundary are maintained in
[Kafka core](../../../kafka-demo/docs/adr/README.md). Current application-specific
behavior is documented in this project's README and authoring guide.

Application-specific decisions:

- [00001: A dedicated consumer creates the notification demo effect](ADR-00001-notification-effect-demo.md)
- [00002: The notification result crosses a second Kafka topic](ADR-00002-confirmation-event-chain.md)
