# First session: Kafka from first principles

This guide describes the delivered flow. Proposed changes from the graduate-audience
review are tracked separately in the [introduction improvement plan](INTRODUCTION_IMPROVEMENTS.md).

## Presentation approach

Open `http://localhost:8080/#/welcome` after refreshing the served assets.
The main deck combines visual explanations with the existing live Kafka demos.
The presenter operates the controls; participants need no homework or application
framework. Explain the scenario before each demonstration. The order example gives
business meaning to the concepts; the live groups are still the configured A/B demo
groups. The notification demo runs a confirmation service and a separate inbox consumer with a second Kafka topic; analytics remains an explanatory example.

## Narrative and live demonstrations

- Motivation, customer-visible pending results, streams, retained history, producer/consumer and record anatomy.
- Live notification demo at `#/demo-record-journey`: Start chain, wait for running,
  then Place order. Order acknowledgment, service consumption, confirmation-event acknowledgment and
  inbox consumption are separate evidence stages. The presenter operates these controls.
- Partition logs and keys → existing `#/partitioning-predict`, `#/partitioning`,
  `#/partitioning-code` sequence. Show the key, partition and offset with real records.
- Ordering explanation → existing `#/ordering-predict`, `#/ordering`, `#/ordering-code`.
  Explain that the sequence control uses A1/B1 application labels, separate from offsets.
- Groups, application instances and scaling → existing `#/groups-predict`, `#/groups`,
  `#/groups-code`. Introduce the demo members and controls before changing membership.
- Input distribution and a new-demo placeholder for key-driven skew.
- Bookmarks → `#/recovery-predict`, `#/lag-predict` and the shared `#/lag` view: stop one group while the other
  continues, then resume from saved commits. This short demonstration is in the main session.
- `#/duplicate-effects`: explain a crash after a business effect but before commit,
  and introduce idempotency. This is a scenario, not a live crash/deduplication capability.
- Benefits, request/response counterexample, `#/client-questions` and an explained
  complete example, ending at `#/first-session-end`.
- `#/optional-labs` → offsets/replay and lag code inspection, in the same deck.

Use the existing predict → run → observe → inspect code → change one thing loop
as a presenter-led demonstration, not an assignment to operate an unintroduced system.
Navigation never starts consumers, produces records or reconnects the shared stream.
The original lab IDs work at the main URL again. `/labs.html` redirects to the same deck.

Only new capabilities have placeholders: see [demo backlog](../BACKLOG.md#first-session-demo-additions).
The main deck's diagrams are explanations, not live telemetry. A consumed card does
not prove a business action completed. The groups workload explicitly rotates
partitions; it does not demonstrate default key distribution.

## Slide talking points

### Accepted now. Confirmed later.

Slide: `#/customer-experience`, immediately after `#/publish-events`.

Agree with the client what “accepted” means: which validation/payment steps must
finish before checkout responds, and how the saved order reliably leads to an
event. Database/publication coordination is not implemented by this illustration.
Explain the pending state, expected delay and what happens if confirmation is
overdue. Kafka acknowledgment alone does not establish checkout success.

### Short recovery demonstration

Slides: `#/recovery-predict`, `#/lag-predict` and the shared `#/lag` view, after group bookmarks.

The A/B workers stand for Notifications/Analytics but only simulate processing.
They do not control the separate notification service. Both scenarios use the same lag dashboard and explicit group controls; navigation never starts workers.

Presenter procedure (allow roughly 3–5 minutes):

1. Observe the experiment topic. Note the existing membership and delay settings.
   For a clean demonstration, stop each A/B group and wait until inactive; stop
   any active workload. Do this explicitly, not by opening the slide.
2. Set each group's delay to 0 ms and start one member in each. Wait for Running
   and all partitions assigned. Keep the start-position policy unchanged.
3. Send the 60-record warm-up. Wait for production to finish and all six displayed
   lag values to reach zero. This establishes valid commits even for an unused group;
   missing/stale values do not count as zero. Do not start the next step without this.
4. Select B, Stop group, and wait for Inactive. Predict whether A will be affected.
5. Send another bounded workload. Wait for production to finish. Observe A at zero
   and B behind on all three partitions. B's saved commits remain at the earlier
   position. The workload rotates partitions explicitly; this is not a key-skew demo.
6. With B still selected, set delay to 200 ms and Add member. Watch B catch up;
   wait for all six lag values to reach zero. Its valid commit determines where it
   resumes; earliest/latest does not override it. No offset reset is needed.
7. Explain: independent groups can recover at different speeds while events remain
   retained. Zero committed-offset lag does not establish an external business effect.
   Restore the prior member counts/delays when finishing the demonstration, with no
   workload left running. New member IDs and advanced commits are expected.

For inspect/change, the optional `offsets-code` slide shows poll → process → commit;
`lag-code` shows the delay and sampled lag. If time permits, repeat with only B's
delay changed. If observations become stale, pause rather than narrating a recovery
that has not been observed. Do not use the notification placement form for this
scenario: it currently requires its service to be running.

### Processing-speed scenario on the same view

Introduce both prediction slides together, then use #/lag for both scenarios.
After the stop/restart scenario, keep both groups running and caught up. Select B,
set its delay to 1000 ms, and send another bounded workload. Watch B fall behind
while A continues. Stop production or let the bounded workload finish, then set B's
delay to zero and observe its commits catch up. Restore the original member counts
and delays after both scenarios. No second dashboard or topic switch is needed.
The first scenario explains saved progress after downtime; the second explains
insufficient processing capacity. Detailed code remains at #/lag-code.

### What if the same event returns?

Slide: `#/duplicate-effects`.

Walk through confirmation sent → crash before commit → event read again. Define
idempotency as avoiding an unwanted repeated business effect. A stable event ID and
durable state are useful, but a separate “seen” flag is insufficient unless the check
and effect are coordinated. External services may accept idempotency keys. Committing
first trades this problem for the possibility of missing an effect. The current
notification inbox is in memory; durable duplicate prevention remains backlog 8.

### Start with the need.

Slides: `#/when-streaming` and `#/client-questions`.

Use a product lookup as an example where an immediate request/response API may be
sufficient. Ask who needs the events, whether retained history is useful, how much
delay is acceptable and who owns overdue/failed work. These are requirements
questions, not a checklist that automatically selects Kafka.

Long notes below are also embedded as hidden `aside.notes`. The current deck does
not bundle Reveal's speaker-notes plugin; keep this guide open separately.

### Something happened.  Who needs to know? 

Slide: `#/welcome`

Introduce streaming through a familiar application and narrate the concepts step by step. Static diagrams explain the concepts; the existing live demos then show Kafka behavior. Placeholders mark only new capabilities still to be built. The presenter operates demos; participants need no application setup.

### A customer places an order.

Slide: `#/order-problem`

Explain that in this illustrative synchronous design checkout waits on downstream calls. Analytics being unavailable can delay or fail that interaction. Adding another downstream application adds another dependency. This motivates the next diagram; it is not a claim that all HTTP integrations have these properties.

### Publish what happened.

Slide: `#/publish-events`

The order service publishes OrderPlaced. Kafka stores it; independent applications read it. Broker acknowledgment is distinct from downstream processing. This sketch assumes the event was successfully published; coordinating database updates and publication is a later topic.

### A stream keeps arriving.

Slide: `#/event-stream`

Compare a nightly batch with continually reacting to arriving data. This timeline illustrates business events, not a global ordering guarantee across Kafka partitions. Kafka Streams is not required to write a basic producer or consumer.

### The reader can catch up.

Slide: `#/retained-history`

A stopped reader may resume while its data remains retained. Retention is finite and configured independently of consumption. A new group needs an appropriate starting position to read history. This is an explanatory log, not the bounded browser history.

### Meet the moving parts.

Slide: `#/moving-parts`

Consumers fetch from brokers; the arrow shows record flow, not broker push. A cluster contains one or more brokers; partitions are hosted on brokers. An application can both produce and consume. The order service is our producer; the notifications and analytics applications are consumers.

### One record. A few useful labels.

Slide: `#/record-anatomy`

The topic is the destination. A record has a key and value and can also have headers. Kafka stores bytes; JSON is one possible application representation. The producer selects a partition and the broker assigns an offset. Timestamp semantics depend on configuration.

### From an event to a notification.

Slide: `#/demo-record-journey`

Implemented as backlog item 18. Start chain starts both consumers; wait for running, then place an order. Follow OrderPlaced on the input topic, service processing, OrderConfirmationPrepared on the output topic, and its receipt by the inbox consumer. The order ID is the key on both topics; the confirmation carries the original event ID as causationId. Point out that a consumer can also produce. The inbox content is read from the second topic, not inserted by the service. Both consumers are hosted in this demo; Start/Stop chain controls them together. The input and output topic names appear below the controls. Stop before clearing the local inbox; clearing changes no Kafka data. All viewers share this runtime.

### One topic. Three ordered logs.

Slide: `#/partition-logs`

Append to a lane, do not move existing records. Offsets in different partitions are not comparable as a global sequence. These short illustrative offsets are contiguous; real observed offsets need not be. Partitions allow work to be distributed.

### Which events belong together?

Slide: `#/choose-key`

With consistent key serialization, the default keyed partitioner and a fixed partition count, the same key maps to the same partition. Different keys can share a partition. This does not sort events by timestamp or fix events published out of business order. Null keys do not imply round-robin.

### Which order matters?

Slide: `#/ordering-meaning`

Explain the two order lifecycles, then connect each sequence to its partition. Kafka preserves the order records were appended within a partition, not a global order across the topic. Concurrent application processing can finish out of order. The following existing live sequence demo shows partition-local read order. Its A1/B1 labels are application sequence labels, separate from Kafka offsets.

### Two applications. Two groups.

Slide: `#/independent-groups`

Notifications sends order confirmations; Analytics updates statistics. Each is a separate application with its own consumer group and progress. Both can read the same retained records, subject to starting position and retention. The diagrams show one consumer instance per application so far.

### An application can run more than once.

Slide: `#/consumer-instances`

Define instance before using scaling diagrams: a running copy of the same application. In this simplified example each copy has one Kafka consumer, and all notifications copies use the same group.id. A worker is not a separate Kafka component; use consumer or instance throughout. Actual applications may contain multiple consumers.

### One application. Several consumers.

Slide: `#/shared-work`

The assignments shown are one valid example, not a deterministic assignment promise. Consumers use the same group.id to cooperate. Stable ordinary consumer groups assign each partition to one member. One member may own multiple partitions.

### Three partitions. Four consumers.

Slide: `#/scaling-limit`

Narrate the rows from top to bottom: one consumer handles all three partitions; additional consumers share them; a fourth has no partition to read. A1–A4 are consumers in Group A. This is a stable ordinary consumer group subscribed to one topic. Exact ownership can differ. These diagrams explain possible assignments, not a running environment.

### Work must spread to scale.

Slide: `#/busy-partition`

The bars illustrate input distribution, not measured throughput. One very busy key maps to one partition for a fixed keyed mapping. That partition remains assigned to one consumer within an ordinary group. More consumers therefore do not guarantee proportional throughput.

### When one key gets busy.

Slide: `#/demo-hot-key`

TODO INTRO-DEMO-4: Add a controlled keyed workload and partition-specific progress observations to show skew and its scaling limit. The existing groups demo already shows assignments and group scaling, but its workload explicitly rotates partitions, so it cannot demonstrate this keyed distribution. Introduce the scenario and run it as a presenter demonstration. See BACKLOG.md.

### Shared history. Separate bookmarks.

Slide: `#/group-bookmarks`

A committed offset is the next offset to read, per group and partition. Explain these as independent saved bookmarks. Consuming does not delete a record. Restarting may redeliver uncommitted work, and a commit alone does not prove an external business effect. Detailed commit policies belong in a later session.

### Would streaming help here?

Slide: `#/when-streaming`

Return to the opening order service. Kafka adds infrastructure, eventual results and failure-handling responsibilities. Simple immediate request/response can still be the right choice. Replay needs appropriate consumer positions; retention is not unlimited. Database/publication consistency, retries and duplicate-safe effects deserve a later session.

### The pieces fit together.

Slide: `#/order-system-recap`

Walk through the completed example. The key relates events for an order, partitions provide parallelism, and each group reads independently. Three partitions illustrate three active notification consumers; this is not a general partition-sizing recommendation. There is no task for participants to perform.

### One event. Many possibilities.

Slide: `#/first-session-end`

Close by returning to the order service: downstream applications can react at their own pace, retain separate progress and scale independently. The main session ends here. All existing live demos remain in this deck; offsets/replay and lag code inspection follow the optional divider. New demonstrations are tracked in BACKLOG.md.

### A closer look at progress.

Slide: `#/optional-labs`

These working demos remain in the same deck. Include them when useful; detailed commit policies and replay controls are beyond the basic first-session narrative. Presenter actions stay explicit.
