# Introduction improvement plan

Reviewed 2026-09-15 against the 37-slide deck served at `http://localhost:8080/#/welcome`.

## Purpose and ownership

Audience: a recently hired graduate with a master's in computer science and little
client-project experience. The deck already explains Kafka's core mechanics well.
This plan strengthens the connection to customer expectations, application code,
failure handling and recovery while preserving the order story and embedded demos.

This is a teaching-quality plan, not a second demo capability backlog:

- **This document** owns audience needs, presentation improvements, relative effort,
  teaching priority and the status of editorial/presentation work.
- **[BACKLOG.md](../BACKLOG.md)** owns demo capability scope, acceptance criteria and
  implementation status. Existing IDs stay stable; feature work is tracked there.
- **[NEXT_DEMOS.md](NEXT_DEMOS.md)** contains detailed plans for the next demo capabilities.
- **[FIRST_SESSION.md](FIRST_SESSION.md)** describes the currently delivered presenter
  flow. Update it when an improvement ships, not merely when it is proposed.

References to implemented backlog capabilities do not mean the corresponding
presentation improvement is done. For example, the replay capability exists, but
including a short recovery demonstration in the main session is proposed here.
This plan does not silently reorder the feature backlog: teaching priority and
feature implementation order answer different questions.

When starting capability work below, extend the linked backlog item and keep its
implementation details/status there. For a scope without its own backlog item,
record it as an extension of the existing capability before implementation. Do not
create a duplicate request. Link completion evidence here when the teaching outcome
is delivered.

## Effort-ordered improvements

The initial batch **INTRO-01, INTRO-04, INTRO-05, INTRO-06 and INTRO-08** is
implemented (2026-09-15); remaining entries are proposed. See delivery notes below.
IDs `INTRO-01`–`INTRO-13` are stable.
Order is estimated effort from least to most, not execution priority or a time
commitment. **P1:** do first; **P2:** valuable next; **P3:** later expansion.
Effort covers the scope described here, so it can differ from broader backlog estimates.

| ID | Improvement and completion outcome | Effort | Priority | Related capability / scope |
| --- | --- | --- | --- | --- |
| INTRO-01 | Clarify demo wording: qualify “no replay” as browser replay. Explicitly map illustrative Group A/B to Notifications/Analytics while explaining that the experiment workers and notification demo are separate runtimes. | Very small | P1 | Presentation wording; backlog 2 and 18. |
| INTRO-02 | Omit the hot-key placeholder from the presented flow until its demo is ready. Keep the planned capability documented and existing functionality accessible. | Very small | P2 | Presentation only; backlog 6 remains planned. |
| INTRO-03 | Make prediction slides ask before explaining. Reveal the answer after the audience predicts and observes, especially for partitioning. | Small | P2 | Existing prediction/demo sequences. |
| INTRO-04 | Add a customer-experience slide after the asynchronous architecture: order accepted now, confirmation later, pending states and acceptable delay. Make clear which work must succeed before checkout responds. | Small | P1 | Editorial; no new runtime. |
| INTRO-05 | Add a duplicate-processing failure story: business action → crash before commit → redelivery. Explain idempotency through a confirmation or payment example, without claiming the current demo implements durable duplicate prevention. | Small | P1 | Conceptual introduction to backlog 8; distinct from INTRO-13's live implementation. |
| INTRO-06 | Expand the Kafka decision recap with a case where request/response is sufficient and client questions about independent readers, useful history, acceptable delay and failure ownership. | Small | P1 | Editorial; no new runtime. |
| INTRO-07 | Add an annotated notification-code walkthrough: decode → validate → business action → checkpoint. Distinguish illustrative code from production code. Flag the database/publication gap and name outbox as a later topic. | Small–medium | P1 | Code explanation grounded in backlog 18; no production outbox implementation. |
| INTRO-08 | Bring a short recovery demonstration into the main session using existing controls: stop one group, continue producing, restart and observe catch-up while another group continues. Establish starting commits and verify the presenter sequence; keep detailed offset resets optional. | Medium | P1 | Reuse implemented backlog 2–4; no new notification behavior. |
| INTRO-09 | Improve dashboard readability: consistent friendly member names across controls and observations, obvious selected observation topic, historical deliveries distinguished from current assignments, and unrelated controls de-emphasized for each lesson. | Medium | P2 | UI refinement of existing demos; preserve explicit commands and navigation-independent consumer lifecycle. |
| INTRO-10 | Add a simple illustrated stream-processing example: continuously updated order totals, a time window and one late-event question. Clearly label it as an explanation rather than live telemetry. | Medium | P2 | Conceptual introduction to backlog 13–14; no processing framework or live topology required. |
| INTRO-11 | Implement the hot-key demo: compare balanced keyed input with one dominant key, then show actual partition distribution and processing/lag evidence for why adding consumers stops helping. | Large | P2 | Capability and status owned by backlog 6 / INTRO-DEMO-4. |
| INTRO-12 | Extend notifications to demonstrate outage and recovery: accept orders while the notification service is stopped, then visibly process the backlog on restart. Define correlation and retained-state behavior before implementation. | Large | P2 | Proposed extension of backlog 18; its current implementation requires a running service before placement. |
| INTRO-13 | Build a controlled crash/redelivery/idempotency demo: expose failure after the business effect but before commit, then demonstrate duplicate prevention with durable state and observable effects. | Largest | P3 | Capability and status owned by backlog 8 and NEXT_DEMOS.md. |

## Recommended first batch

**INTRO-01, INTRO-04, INTRO-05, INTRO-06 and INTRO-08.** These provide the strongest
connection to real application work with little new demo infrastructure. This is
a recommendation, not an instruction to implement all entries automatically.

Preserve the existing strengths: one order story, the acknowledgment/consumption/
result distinction, clear partition/group diagrams, and predict → run → observe →
inspect code → change one thing. Introduce each demo by saying what to watch and
which change matters. Keep technical caveats accurate without making every control
or qualification compete for attention at once.

## Review evidence and limits

The review covered all served slide content, presenter notes, selected implementation
details, rendered demo screens and current runtime observations. No new workloads
were triggered and no consumer membership was changed. It was a teaching review,
not a fresh functional test. Effort and priority are judgments for this audience.

Success means a newcomer can explain how Kafka distributes retained events, why
that can help an application, and which responsibilities the application still has.

## Initial batch delivered, 2026-09-15

- **INTRO-01:** the lesson status says “no browser replay”; groups/recovery explicitly
  map A/B to example business roles and distinguish the separate notification service.
- **INTRO-04:** `customer-experience` explains accepted/pending/ready and acceptable delay.
- **INTRO-05:** `duplicate-effects` explains the crash window and idempotency. The live
  crash/durable deduplication capability remains unimplemented in backlog 8.
- **INTRO-06:** the recap includes a request/response counterexample; `client-questions`
  covers readers, history, delay and failure ownership.
- **INTRO-08:** `recovery-predict` and `lag-predict` lead into one shared `lag` dashboard
  before the main ending. Stop/restart and processing-speed scenarios use the same
  controls and history; the duplicate recovery dashboard has been removed.

The deck now has 41 slides. The [presenter guide](FIRST_SESSION.md) records setup,
warm-up commits, observation limits and cleanup. Verification and runtime delivery
details are recorded in [CONTEXT.md](../CONTEXT.md). Existing feature-backlog
implementation status and ordering are unchanged.
