# Next demos: load, duplicate effects, and safe retries

Proposed 2026-09-10. This is an implementation plan, not delivered functionality
or an accepted architecture decision. Backlog IDs remain 6, 8 and 9.

## Teaching sequence

Use one domain throughout: a purchase awards loyalty points to a customer.
A Kafka key identifies the customer; a stable event ID identifies one award.
Two different awards for the same customer must both take effect. Republishing
one award with the same event ID must not award points again in the safe handler.

| Demo | Prediction | Main visual | One change to try |
| --- | --- | --- | --- |
| Hot keys | Will more consumers fix one overloaded customer key? | Key distribution, actual partition traffic, assignment and lag | Balanced keys → 90% one key; then add members |
| Failure and duplicate effects | What happens if an award is stored but its Kafka offset is not committed? | Delivery → database effect → Kafka commit timeline, beside a points ledger | Unsafe handler → idempotent handler; repeat the same failure |
| Retries | Which failures should we retry, and what happens to following records? | Attempt timeline, blocked records, successful effects and dead-letter records | Transient failure → permanent failure with bounded attempts |

Each demo gets prediction, live experiment, and inspect/change slides. The
failure demo also gets two short idempotency prose slides. Reuse membership,
per-group delay and the application socket throughout.

## 1. Hot keys and uneven load (medium)

1. Run three partitions and one group member with a fixed per-record delay.
   Send a bounded workload using a deterministic set of many customer keys.
2. Show the actual acknowledged partition distribution. This baseline is expected
   to be reasonably spread, not guaranteed to be exactly even.
3. Keep count, rate, payload size and delay fixed. Send 90% of records under one
   customer key, with the rest spread across the same key set.
4. Increase the group to three members. Observe the hot partition's owner and its
   lag while other members do less work. A fourth member illustrates the existing
   partition parallelism limit.
5. Inspect key selection. Discuss a better domain key where it preserves the
   required ordering. Arbitrary salting can split related events and change ordering
   and aggregation requirements; don't present it as a free optimization.

The current workload explicitly rotates partitions. Add a named keyed workload
mode that omits the explicit partition argument. Preserve the existing explicit
partition mode as a clearly labelled teaching control. Use producer acknowledgments
for per-run distribution counts, not the latest browser cards or the 48-event ring.
Show the chosen key mix beside real partition outcomes. Sample Kafka lag separately;
it includes retained work outside the current run if the group was already behind.

Compare runs after catch-up, or visibly mark a run that began with backlog. Partition
count and partitioner stay fixed. Keep timings illustrative rather than benchmark
claims. A quiet B group can provide an optional independent comparison; don't
change delay and member count simultaneously in the first experiment.

**Acceptance:** same-key sends acknowledge the same partition in the chosen setup;
counts match acknowledged records; added members do not split the hot partition's
ownership; no claim that different keys must choose different partitions. A failed
send is marked uncertain and is not automatically retried.

## 2. Failure, duplicate effects, and idempotency (medium–high)

### Effect and identity

Proposed event: `{eventId, customerId, points}` with positive integer points and
`customerId` as the Kafka key. Preserve eventId across redelivery and deliberate
republishing; new legitimate awards receive new IDs. Display both logical event ID
and Kafka topic/partition/offset. Offset identity alone will not recognize a
republished logical event at another offset.

Introduce a tiny transactional points ledger in a file-backed embedded database
(proposed choice, to be recorded in an ADR before implementation). It supplies real,
inspectable state without requiring a new server container. Store processed IDs and
points effects in the same database transaction. A unique constraint must arbitrate
concurrent attempts; a check followed by an unprotected write is insufficient.

Scope ledger state by explicit run ID and logical handler/group so A and B can
independently process the same award. Include a payload fingerprint with processed
IDs: the same ID with different award data is a conflict, not a harmless duplicate.
Run creation/selection is explicit and survives worker restart. Cap events per run;
never evict deduplication records silently to bound the browser. Cleanup is an
explicit inactive-run action affecting ledger and deduplication state together.

### Presenter sequence

1. Start with a single member and the unsafe handler: each delivery adds 10 points.
2. Arm a one-shot fault for a selected event, after the ledger transaction commits
   but before Kafka commit. Show 10 points, an unadvanced Kafka commit, and a stopped
   worker. This is controlled worker termination, not a claimed machine crash.
3. Restart the worker. Observe the same Kafka record again and a total of 20 points.
4. Use a fresh explicit comparison run with the idempotent handler, then repeat the
   same fault. The second attempt is recognized and the total remains 10 points;
   Kafka progress can now be committed.
5. Republish the same logical event ID at a new offset. The safe handler still
   applies no extra effect. Send a new event ID for the same customer: it adds points.

Use a small failure matrix in prose: before effect → safe to try again;
after effect/before Kafka commit → duplicate-effect risk;
after Kafka commit → normal restart resumes later. Faults are one-shot and tied to
run/group/event, so restarting does not create an accidental endless crash loop.
Don't change handler mode against a dirty ledger to manufacture a comparison.

**Evidence:** separate attempt, effect-applied, duplicate-skipped, identity-conflict,
fault-fired and confirmed-commit observations. Query ledger state for totals rather
than inferring effects from consumed cards. Database commit and Kafka commit remain
separate; idempotency makes the intervening replay safe for this database effect.

**Acceptance:** unsafe replay yields two effects; safe replay yields one; new event
IDs still apply; duplicate IDs at new Kafka offsets are recognized; conflicting
payloads fail visibly; concurrent duplicate attempts apply once. Verify transaction
rollback leaves neither an effect nor a processed marker. Verify ledger and
processed IDs survive an application restart as well as worker restart.

### Draft prose: idempotency as a design habit

> Design handlers so that doing the same work again leaves the same business
> outcome as doing it once. A redelivery can then be an ordinary recovery step.
> The important question is which effect must happen only once, and how we
> recognize that it has already happened.

> For a points award, “add ten” is not idempotent. Give the award a stable identity
> and record that identity atomically with the points change. A second legitimate
> award gets a new identity; a retry keeps the old one.

Add speaker notes: an idempotent state assignment can be simpler than a deduplication
ledger, but old events may overwrite newer state without a version/order check.
Idempotency does not solve ordering, invalid input, or overload. Deduplication needs
an explicit retention horizon. External email/payment APIs require their own
idempotency support or another coordinated delivery design; our database transaction
does not cover a remote service. Kafka producer idempotence is a different boundary
from application effect idempotency. Keep outbox implementation and Kafka transaction
lessons outside this increment.

## 3. Retries and dead-letter handling (medium–high)

Build on the same idempotent handler and ledger, rather than introduce another domain.

1. Inject a transient failure before the effect: fail twice, then succeed on the
   third attempt. Show attempt count, bounded backoff and exactly one ledger effect.
2. Inject a permanent invalid award. Retry only eligible failures; permanent
   validation failures can go directly to dead-letter handling under the chosen
   policy. Contrast this with exhausting a transient failure's retry budget.
3. Show subsequent records waiting behind the unresolved record. Begin with bounded
   in-worker retries: other partitions owned by that worker may also wait, while
   other workers can proceed. Label that scope explicitly.
4. After exhaustion, publish to a pre-created allowlisted dead-letter topic with
   original event identity, source coordinates, failure reason and attempt count.
   Advance source progress only after confirmed DLT publication under this policy.
5. Optional presenter-triggered redrive after repairing the failure cause: retain
   logical event ID. Replaying a dead-letter record is an operational decision, not
   automatic evidence that the original problem has been fixed.

Use short bounded delays that fit within the configured poll interval, with a
visible backoff timeline. Retry-topic routing and scheduling are later extensions:
letting later records overtake a failed one changes the ordering story.

DLT publication and source commit are also separate operations in this first
implementation. A failure between them can duplicate a DLT record. State this
explicitly and preserve source/event identity for downstream deduplication. Failed
or ambiguous DLT publication must not be shown as successful recovery. Skipping a
record to DLT allows progress but does not complete its business effect.

**Acceptance:** transient failure eventually applies once; permanent failures follow
policy; budgets prevent endless retry; later records are not committed past an
unresolved predecessor in the same partition. Handle the remainder of an already
polled batch explicitly (retain it or seek back) so a failure cannot silently drop
records. Test DLT failure, the DLT-ack/source-commit gap, and redrive with unchanged
identity. Explain that in-memory retry counts can restart after worker failure;
do not claim a durable lifetime attempt limit without storing it.

## Implementation boundaries and delivery

- Deliver hot-key workload and visualization first. Then implement the real effect
  boundary and safe/unsafe handlers, followed by failure injection and prose.
  Add retry/DLT behavior only after effect idempotency is verified.
- Extract a small record-processing interface from the existing worker loop when
  the ledger handler needs it. Keep polling, assignment and Kafka commits owned by
  the runtime; handlers report outcomes and never operate KafkaConsumer directly.
  Keep retry policy and one-shot faults separate from effect storage.
- Preserve existing delay-only lessons as the default mode. Select an effect mode
  explicitly while the target group is stopped. The backend validates run/group
  scope and owns configuration; mounting a concept performs no experiment actions.
- Add structured run-scoped snapshot fields for counters, ledger summaries and
  attempt/fault state. Keep the event ring bounded and the browser read-only apart
  from explicit presenter commands. New views register through onExperiment;
  reuse membership controls rather than copy them.
- Version incompatible experiment payload changes explicitly, updating frontend,
  tests and authoring guide together. Record accepted runtime/storage decisions in
  an ADR during implementation; this proposal does not supersede current ADRs.
- Use isolated embedded-broker/database tests for effect atomicity and commit
  boundaries; live browser checks for the teaching sequence and singleton socket;
  inspect rendered slides at presentation and laptop sizes. Preserve user runtime
  settings when refreshing the application.

## Reference notes

The transactionally stored processed-message identity follows the
[Idempotent Consumer pattern](https://microservices.io/patterns/communication-style/idempotent-consumer.html).
Kafka's discussion of delivery semantics explains why output to an external system
needs coordination beyond Kafka offsets alone:
[Kafka design: delivery semantics](https://kafka.apache.org/38/design/design/).
These support the correctness boundaries above; the lesson sequence and proposed
implementation choices are specific to this repository.
