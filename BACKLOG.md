# Kafka lesson backlog

Updated 2026-09-10. Rows 1–4 are implemented; remaining rows are proposals.
The implementation plan below records the intended teaching behavior and acceptance checks.
Source material: the sibling [workshop exercises](../kafka-workshop/exercises/),
the current partitioning deck, and [the authoring guide](docs/ADDING_LESSONS.md).

Each row can span prediction, experiment, and code slides. Complexity covers a
working visual lesson, including missing backend observations: **Low** reuses the
current stream; **Medium** adds focused controls or instrumentation; **High**
requires substantial processing or infrastructure. Ordering is the recommended
historical ordering; rows 1–4 are complete. The next planned batch is rows 6, 8
and 9; see the revised priorities below.

| Topic | Brief description of lesson taught | Rough estimate of complexity |
| --- | --- | --- |
| ✓ **1. Ordering and partition-local offsets** | Extend the existing lesson with numbered events in partition lanes. Show why offsets describe order within a partition, without establishing a global order across partitions. Exercises 1, 3. | **Low** — reuse existing record metadata and producer. |
| ✓ **2. Consumer groups and parallelism** | Draw connections between partitions and consumers. Add consumers until some remain idle; add a second group to show independent consumption of the same log. Exercises 2, 3. | **Medium** — controlled demo consumers plus group/member and assignment observations. |
| ✓ **3. Offsets, commits, and replay** | Put consumed position and committed offset on a timeline. Stop, restart, and reset a dedicated group to show where reading resumes; explain when earliest/latest applies. Exercises 1, 2. | **Medium** — commit/position observations and explicit restart/reset controls. |
| ✓ **4. Consumer lag and catching up** | Show the distance between log end and committed offset growing as processing slows, then shrinking during recovery. Separate fetched records from completed work. Exercise 11. | **Medium** — broker/group offset sampling, processing signals, adjustable workload. |
| **5. Rebalancing** | Join or stop a consumer and animate observed partition ownership changes. Show the transition and recovery rather than only the final assignment. Exercises 2, 3. | **Medium** — reuse the group lab; add assignment/revocation events and lifecycle controls. |
| **6. Hot keys and uneven load** | Send mostly one key and watch one partition dominate. Explore why adding consumers may leave the bottleneck unchanged. Extension of exercise 3. | **Low–Medium** — keyed workload presets and distribution counters; processing/lag instrumentation for a measured bottleneck. |
| **7. Null keys, sticky partitioning, and batching** | Predict whether null-key sends rotate partitions. Compare observed distribution while changing payload volume and producer settings; explain the latency/throughput tradeoff. Already identified in the demo roadmap. | **Medium** — workload/configuration controls and producer metrics; record cards alone cannot expose batch boundaries. |
| **8. Processing failures and duplicate effects** | Crash after an effect but before committing, then observe redelivery. Add an application-level deduplication step and compare the result. Extension of exercises 2, 7. | **Medium–High** — controlled failure points, observable effects, commits, and deduplication state. |
| **9. Retries and dead-letter topics** | Follow a poison record through failed attempts and eventual dead-letter publication. Show how the chosen error policy affects subsequent records. Exercise 7. | **Medium** — failure injection, attempt events, and dead-letter observations. |
| **10. Event log → current state** | Place an append-only event history beside a table of the latest observed value per key. Show how updates and tombstones change the table. Bridge between exercises 5 and 9. | **Low–Medium** — a second view of existing records plus explicit null-value production. Label state as built from observed events. |
| **11. Retention, compaction, and tombstones** | Compare deleting old log segments with removing superseded keyed records. Re-read a dedicated topic to observe what remains and why offsets can have gaps. Exercise 5. | **Medium–High** — dedicated topics, cleanup configuration, bounded rereads, and asynchronous cleanup timing. |
| **12. Serialization and schema evolution** | Visualise object → bytes → object. Compare old/new writer and reader schemas, predicting which changes work under a chosen compatibility policy. Exercise 4. | **Medium** — Avro/Schema Registry integration and visible compatibility/deserialization outcomes. |
| **13. Stream transformations and windowed counts** | Follow orders through filter, re-key, and count stages into an output topic. Use time buckets to make aggregation concrete. Exercises 8, 9. | **High** — a real processing topology and correlated input/output observations; start with one framework. |
| **14. Event time, late events, and watermarks** | Send events out of timestamp order and compare event-time windows with processing-time windows. Show the configured treatment of late arrivals. Flink extension in exercise 8. | **High** — timestamp-controlled input and window/watermark observations. |
| **15. Kafka Connect: topic → database** | Follow an event into a database row, pause the connector, and observe backlog and recovery. Exercise 6. | **High** — Connect, database, and evidence of actual sink writes. |
| **16. Replication, acknowledgments, and broker failure** | Show leaders, replicas, and ISR; stop a broker and observe availability under different acknowledgment settings. Extension of exercise 11. | **High** — multi-broker infrastructure, controller-topology decisions, and broker-state instrumentation. |
| **17. Authentication and authorisation** | Visualise identity checks separately from permission checks. Predict which produce/consume operations succeed for different principals. Exercise 10. | **Medium–High** — authenticated clients, ACL scenarios, and observable failures. |

## Next batch and revised priorities

[Detailed demo plan](docs/NEXT_DEMOS.md): hot keys → failure/duplicate effects
with idempotency → retries/dead-letter handling. These remain unimplemented.

Then: current state (10), compaction/retention (11), producer batching (7), schema
evolution (12), windowed processing (13), and event time (14). Replication (16)
is gated on multi-broker infrastructure; Connect (15) and authentication (17)
remain optional later tracks. IDs above stay stable for references.

Treat rebalancing (5) as an extension of the group lesson for now. A dedicated
lesson should add deeper transition/failure observations rather than repeat the
membership controls already delivered.

## Completed work

- [x] Lesson 1: ordering and partition-local offsets.
- [x] Lesson 2: consumer groups and parallelism.
- [x] Lesson 3: offsets, commits, and replay.
- [x] Lesson 4: consumer lag and catching up.
- [x] Shared membership widget on groups, offsets, and lag.
- [x] Per-group processing delay in the shared widget and backend.
- [x] Architecture decisions and authoring documentation (ADRs 00007–00008).

Verification details are recorded in [CONTEXT.md](CONTEXT.md). Rebalancing
observations support the current group lesson; deeper work in row 5 remains
open as an extension rather than the next standalone lesson.

## Original implementation plan for lessons 1–4

Implement in order: ordering → a shared group experiment backend and group lesson
→ commits/replay → lag. Lesson 1 can ship independently. Lessons 2–4 reuse one
small, explicit experiment runtime; their combined scope is larger than four
frontend slide additions.

### Shared design for lessons 2–4

- Keep the existing observer consumer and application-level browser connection
  independent of experiment consumers. Opening a slide, clearing a display, or
  reconnecting the browser must not change Kafka membership or replay records.
- Use a pre-created, dedicated three-partition exercise topic and a small allowlist
  of demo group IDs, separate from the observer group. Read actual partition
  inventory and group state; do not discover partitions only from arriving cards.
  Configure topics at startup using the existing approach.
- Add a backend-owned experiment controller with explicit start/stop member,
  group selection, workload, and later replay controls. Cap members and workload;
  disable conflicting operations while transitions are in progress. Experiment
  state is shared between viewers and survives slide navigation. Stop workers on
  backend shutdown; show their actual state when a viewer reconnects.
- Prefer a small dedicated Kafka consumer loop for teaching poll/process/commit,
  with consumer operations confined to its owning thread. The existing Spring
  listener remains the observation path. Record this proposed architectural choice
  in an ADR before implementation; keep the teaching loop readable in source.
- Extend the versioned observation protocol only as needed. Group/member identity,
  assignment, record delivery, processing completion, and confirmed commit are
  distinct observations. Include topic, partition, offset, run identity, and sample
  time where relevant. Commit failure must never appear as commit success.
- Deliver experiment events through the existing application transport, with a
  bounded history and current-state snapshot for initial connection/reconnection.
  A snapshot restores the display, not Kafka consumption history. Distinguish
  observer cards from experiment-member deliveries and mark stale/unavailable data.
- Extract root-scoped producer/control wiring when the second lab needs it. Add
  stable slide IDs, explicit event routing for the new concepts, and update the
  authoring contract. Do not add navigation-triggered consumers or a general
  administration framework.

### 1. Ordering and partition-local offsets

**Prediction:** If we interleave two keyed sequences, which order can we rely on?

**Slides:** prediction → partition lanes → inspect/change. Extend the existing
partitioning lesson instead of repeating its introductory material.

**Experiment:** Send a short numbered sequence for one key, then interleave a
second key. Preserve explicit presenter-triggered production and actual producer
send order. Show payload sequence labels separately from Kafka offsets. Different
keys may share a partition; choose example keys from observed acknowledgments if
the demonstration needs different lanes.

**Evidence:** Existing topic/partition/offset/key/value observations suffice.
Compare offsets only within a partition. Do not imply offsets must be contiguous,
that payload sequence numbers define Kafka order across producers, or that browser
arrival order establishes a global Kafka order.

**Code change:** Change the key while holding the payload sequence constant; show
the relevant producer and consumer mapping code.

**Acceptance:** The visual makes partition-local order clear, keeps bounded
history, handles literal/null payloads, and retains one socket through navigation.
Use stable slide IDs in affected browser tests and inspect presentation/laptop
layouts. No backend behavior change is needed.

### 2. Consumer groups and parallelism

**Prediction:** With three partitions, what changes when we run one, two, and four
members? What changes when we start another group?

**Slides:** group model → assignment experiment → inspect/change.

**Experiment:** Start group A with one member, increase to two and then four, and
wait for observed stable assignments at each step. Produce a bounded workload
covering all partitions. Start group B with a defined initial offset policy, then
produce a fresh numbered workload that both groups can observe.

**Evidence:** Draw actual assignment edges and member-specific deliveries,
including idle members and transitional states. Each partition has one owner per
group in a stable assignment. Different groups independently consume the same
records. Two browsers remain viewers of this experiment, not group members.

**Code change:** Show group ID and member-count configuration. Do not promise a
particular assignment algorithm or equal processing load.

**Acceptance:** Live broker checks confirm stable ownership, an idle member above
the partition count, and both groups receiving the fresh workload. Navigation and
browser reconnect do not alter membership. Failed starts/stops leave controls and
reported state usable. Detailed rebalance timing remains lesson 5.

### 3. Offsets, commits, and replay

**Prediction:** After processing records and restarting a group, where does it
resume? Does changing auto-offset-reset override an existing commit?

**Slides:** offset timeline → restart/replay experiment → inspect/change.

**Experiment:** On a dedicated group, process a bounded sequence and confirm its
commit. Stop and restart to demonstrate resume. Then stop every member, reset to a
validated offset within the retained log, confirm the reset, and restart to
observe redelivery. Compare earliest/latest using fresh allowlisted groups without
committed offsets. Show the actual selected start positions.

**Evidence:** Use separate markers for consumer position (next fetch position),
completed processing progress, and confirmed committed offset (next resume
position). Position may advance beyond processing because poll can return a batch.
Offsets and progress remain partition-specific. Display reset and WebSocket
reconnect must visibly remain different operations from Kafka offset reset.

**Code change:** Inspect poll → process → commit and initial offset configuration.
Start with an explicit commit after a successfully processed batch; defer crash
windows and duplicate side effects to lesson 8.

**Acceptance:** Restart resumes from confirmed commits; a reset on an inactive
group causes the selected retained records to be delivered again. Reject resets
while members are active or offsets are out of range. Verify commit failures do
not move the confirmed marker and reset failures do not claim success.

### 4. Consumer lag and catching up

**Prediction:** What happens when production outruns processing? After production
stops, how can we tell the consumer has caught up?

**Slides:** backlog prediction → per-partition lag experiment → inspect/change.

**Experiment:** Reuse the group runtime with a bounded, presenter-started producer
workload. Increase a simulated processing delay, observe backlog, stop production,
then reduce the delay and watch recovery. Keep the first experiment within poll
interval limits so it teaches lag rather than accidental membership loss.

**Evidence:** Sample log end and committed offsets per partition; label their
difference as committed-offset lag, not a count of unfinished business operations.
Show sample timestamps, processing completions, and commit steps so delayed commits
are distinguishable from slow processing. An absent commit is unknown/uninitialised,
not zero. Samples are not atomic; do not conceal inconsistent or stale readings.
Use a bounded chart and display sampling failures without freezing a healthy state.

**Code change:** Change processing delay, then compare production and processing
rates. Keep commit policy constant initially; use a later comparison to explain
why committed lag can fall in steps.

**Acceptance:** With initialized commits and a controlled workload, lag grows
under slower processing and eventually reaches zero after production stops and
processing/commits catch up. Confirm values against broker group offsets. Test
missing/stale samples and control failures. Do not infer business completion from
zero lag alone.

### Delivery and verification

1. Deliver lesson 1 with proportional browser/layout checks.
2. Add the shared experiment runtime and lesson 2 together, with meaningful backend
   tests for membership lifecycle and event attribution plus a live broker check.
3. Add lesson 3 with restart/reset/commit integration coverage.
4. Add lesson 4 with offset-sampling and workload checks, then run the four-lesson
   teaching sequence against the actual local broker.

For each increment, verify that navigation preserves the single shared connection,
controls are scoped correctly, display history stays bounded, and observation
failures remain visible. Check current runtime state before rebuilding/restarting;
do not assume dated workshop infrastructure observations are still true. Update
README for run/setup changes, CONTEXT for delivered capabilities, the authoring
guide for contract changes, and an ADR for the experiment architecture. Keep this
file's status current as lessons ship.
