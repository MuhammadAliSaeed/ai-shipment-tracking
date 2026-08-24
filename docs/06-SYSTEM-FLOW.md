# 06 — System Flow

This document traces one exception from a carrier event to a resolved action, naming the component responsible at each step and the invariant it upholds. Re-read it whenever you lose the thread of how the pieces connect.

---

## 1. The four flows

The system has exactly four flows. Almost every question about "where does X happen" resolves to one of them.

| Flow | Trigger | Ends at | Latency |
|---|---|---|---|
| **A — Ingest** | A carrier event arrives | Shipment state updated | < 1s |
| **B — Detect** | State changes | Exception raised, or nothing | < 100ms |
| **C — Resolve** | Exception raised | Action executed or escalated | seconds → days |
| **D — Learn** | An outcome is known | Eval scenario, calibration update | offline |

Flow A runs on every event. Flow B runs on every event and produces nothing most of the time. **Flow C is where the agents live and where the money is spent — it runs rarely by design.** Flow D is what keeps the system from silently degrading.

The economic shape of the system is entirely determined by how selective Flow B is.

---

## 2. Flow A — Ingest

```
Carrier / Mock
     │  raw payload
     ▼
┌─────────────────────┐
│ Webhook receiver    │  verify signature → produce to shipment.events.raw
│ (FastAPI)           │  never blocks on downstream work
└─────────┬───────────┘
          ▼
   Redpanda: shipment.events.raw          (key = shipment_id)
          │
          ▼
┌─────────────────────┐
│ Ingestor            │  1. adapter.normalize()   deterministic, no LLM
│                     │  2. insert_event_if_new() unique(idempotency_key)
│                     │  3. produce normalized
│                     │  4. signal-with-start the shipment workflow
└─────────┬───────────┘
          ▼
   ShipmentLifecycleWorkflow(shp-SHP-10432)   ← durable, alive for weeks
```

### What matters here

**The receiver does nothing but verify and enqueue.** It never calls the database, never starts a workflow, never runs an agent. Carriers time out and retry aggressively; a slow receiver turns one event into five duplicates. Accept fast, process asynchronously.

**Deduplication happens at step 2, before the workflow is signalled.** The unique constraint on `idempotency_key` is the enforcement point. Filtering later would mean the duplicate is already permanently recorded in the workflow's event history.

**`key = shipment_id` on every produce.** Kafka guarantees ordering within a partition, so this gives per-shipment ordering — which is exactly the granularity where order matters and exactly the granularity where writes must serialize.

**Signal-with-start is atomic.** It starts the workflow if absent and signals it either way, in one operation. A separate "check then start" would race under load and create duplicate workflows for the same shipment.

*Invariants upheld: 1 (durable workflow), 4 (per-shipment ordering).*

---

## 3. Flow B — Detect

```
ShipmentLifecycleWorkflow wakes on signal
     │
     ▼
state = state.apply(event)                 pure function, replay-safe
     │
     ▼
detect_exceptions(state, event)            5 deterministic rules, ~2ms, NO LLM
     │
     ├── nothing found ──────────────────► sleep until next event or 6h timeout
     │
     └── exception found
              │  dedupe_key already open? ──► drop
              ▼
        start_child_workflow(ExceptionResolutionWorkflow)
```

### What matters here

**This layer is deterministic, and that is the entire economic argument of the system.** Detection runs on every event — thousands per day. If it involved a model call, cost would scale linearly with event volume and the system would never be viable. Because it is five rules running in about two milliseconds, the expensive reasoning tier is reached only when something is actually wrong.

**Silence is handled by the timeout, not by an event.** The `wait_condition(..., timeout=6h)` fires when nothing arrives. A purely event-driven system cannot detect the absence of events, and absence is one of the strongest disruption signals in logistics.

**`dedupe_key` prevents alert storms.** A shipment sliding from 13 to 14 to 15 hours late is one exception, not three, because the key buckets by 12-hour bands. Alert fatigue is engineered away here, in the detector — not managed later through operator training.

*Invariants upheld: 3 (bounded — cheap layer gates the expensive one).*

---

## 4. Flow C — Resolve

This is the multi-agent core. Six stages.

```
ExceptionResolutionWorkflow(exc-7f3a)
  budget: $2.00 / 150k tokens / 20 calls
  │
  ├─ STAGE 1  PARALLEL READS ────────────── asyncio.gather, 5 activities ──┐
  │     agent.signal_interpreter    fast    "AWAITING_BERTH means..."      │
  │     agent.disruption_context    fast    "Rotterdam severe + storm"     │
  │     agent.route_risk            fast    "vessel holding 12nm offshore" │
  │     agent.customs_docs          fast    "docs complete, no hold"       │
  │     agent.impact_assessor       fast    "$15,480 exposure, strategic"  │
  │                                                          ~1.8s total ──┘
  │
  ├─ STAGE 1b  predict_eta                  deterministic, 8ms, NO LLM
  │
  ├─ GATE      warrants_action(eta)?  ──── no ──► no_action, DONE (~$0.03)
  │
  ├─ STAGE 2  agent.remediation_planner     reasoning tier
  │              └─ sandbox.execute         cost simulation, network isolated
  │              → 3 options + do_nothing_consequence
  │
  ├─ STAGE 3  agent.critic                  reasoning tier, CONTEXT-ISOLATED
  │              input: evidence + options   (NOT the planner's reasoning)
  │              → approved / rejected
  │
  ├─ STAGE 4  policy.evaluate (OPA)         deterministic, 6ms
  │              tier=T3, cost $1,200 > $250 → requires_approval = true
  │
  ├─ STAGE 5  DURABLE HUMAN WAIT ─────────── hours or days, zero resources
  │              approval card → operator → signal → resume
  │
  └─ STAGE 6  ActionBroker.submit()         the ONLY write in the entire flow
                 idempotency → policy → persist → lock → execute → audit
```

### Stage 1 — why parallel here is correct

These five agents read **independent** sources. Nothing the disruption agent learns changes what the customs agent should look at. That independence is what makes the fan-out safe, and it is why this domain was chosen to teach multi-agent architecture.

The contrast is worth holding onto: in a **write-heavy** domain such as code generation, parallel agents produce incoherence, because each carries implicit decisions the others cannot see. Reads fan out; writes serialize. The same rule, both halves visible in one system.

Operationally, each agent is its own Temporal activity with its own timeout and retry policy. If `route_risk` fails, Temporal retries only `route_risk`. The other four results are already durably recorded and are not recomputed.

### Stage 1b/GATE — the economic control

Cheap tier always. Expensive tier conditionally. An exception that turns out not to warrant action costs about three cents and stops here. This gate is what keeps cost per exception under target, and no budget cap can substitute for it.

### Stage 2 — planning, with a sandbox

The planner may write and execute a short computation to compare reroute options. That execution goes through the hardened, network-isolated sandbox. The moment an LLM emits executable code you are in the code-generation threat model, regardless of what your product does.

Its output is forced to include `do_nothing_consequence`. Agents are biased toward action; asked for options, they produce options. Requiring the null-action counterfactual is a cheap structural correction, and it is frequently what a human ends up choosing.

### Stage 3 — the critic, and why isolation is the whole mechanism

The critic receives `assessments.evidence()` and `options`. It does **not** receive the planner's reasoning chain.

This is not a minor detail — it is the entire source of the critic's value. Given a persuasive argument, a model tends to agree with it. Given raw evidence and a claim, it can actually check whether one supports the other. Pass the planner's reasoning through and you have built an expensive rubber stamp, which the `CriticRubberStamping` alert in Phase 4 exists to detect.

### Stage 4 — policy, not code

OPA answers two questions: what tier is this action, and may we act without a human. Autonomy at T3 requires cost under threshold **and** confidence above 0.85 **and** critic approval **and** a non-strategic customer. Conjunction, never a single signal.

The policy also returns *reasons*, which flow onto the approval card so the operator knows why they specifically were asked.

### Stage 5 — the durable wait, and why Temporal was worth it

```
Workflow suspends.  Zero CPU. Zero memory. Zero cost.
  ↓
Operator opens the card 4 hours later — after a deploy, after a restart,
possibly after the entire stack was down for maintenance.
  ↓
POST /approvals/{id} → temporal signal → workflow resumes at exactly this line.
```

Compare the alternatives: a blocking HTTP request dies on the next deploy; a polling loop burns money proportional to wait time; a database flag plus a cron job is a state machine you now maintain. This single primitive is the payoff for choosing a heavier orchestrator in Phase 1.

If nobody responds within the timeout, the workflow escalates. It does not hang forever, and it does not silently act.

### Stage 6 — the single write

Everything upstream produced an inert `ActionProposal`. Only the broker can act, and it does so in a fixed order: **idempotency check → policy → persist as proposed → per-shipment lock → execute → audit**.

Persisting *before* executing is what makes a mid-flight crash recoverable. The per-shipment advisory lock — not a global lock — means different shipments proceed in parallel while the same shipment never does.

*Invariants upheld: 1 (durable wait), 2 (one trace), 3 (budget), 4 (fan-out then serialize), 5 (propose vs dispose).*

---

## 5. Flow D — Learn

```
Real outcome known (delivered / breached)
     │
     ├─► append to evals/scenarios/  with ground_truth
     ├─► human approve/reject decisions → quality signal
     └─► confidence vs correctness   → calibration report
                │
                ▼
     make eval  →  CI gate  →  Rego threshold tuning  →  prompt/model change
```

Two loops close here, and both matter:

- **Calibration → policy.** If the 0.9 confidence bucket is right only 60% of the time, the 0.85 autonomy threshold in Rego is granting autonomy on unreliable signals. Either recalibrate the agent or raise the threshold. This is what makes `confidence` a real control rather than a decorative float.
- **Human rejections → quality.** A rising human rejection rate between eval runs means something regressed that the golden suite did not catch — which is itself a finding, and tells you which scenario to add next.

*Invariants upheld: 2 (observable, therefore improvable).*

---

## 6. Worked example

**Setup:** `SHP-10432`, ocean, Shanghai → Rotterdam, promised 14 March 08:00, priority SLA, strategic customer, $180/hour penalty.

| T+ | Component | What happens |
|---|---|---|
| 00:00.0 | Mock carrier | Emits `AWAITING_BERTH` at NLRTM, `event_time` 11 Mar 09:00 |
| 00:00.1 | Webhook receiver | Verifies, produces to `shipment.events.raw`, returns 200 |
| 00:00.3 | Ingestor | Normalizes → `AT_HUB`, raw kept. New key, inserted. |
| 00:00.4 | Ingestor | Signal-with-start → `shp-SHP-10432` |
| 00:00.5 | Lifecycle workflow | Applies event. `detect_exceptions` → **delay, high**. Predicted 17 Mar vs promised 14 Mar. |
| 00:00.6 | Lifecycle workflow | Starts child `exc-7f3a`. Root trace opens. |
| 00:00.7 | Mock carrier | **Duplicate** of the same event arrives |
| 00:00.8 | Ingestor | Unique constraint rejects it. `duplicate_events` incremented. **Nothing downstream sees it.** |
| 00:01.0 | Resolution workflow | Stage 1: five agents dispatched concurrently |
| 00:02.8 | — | All five return. Signal: `major_delay`, 0.88. Disruption: `severe`, 76h wait. Impact: **$15,480**, strategic. |
| 00:02.9 | ETA predictor | Deterministic: 17 Mar 22:00. 86 hours late. |
| 00:02.9 | Gate | `warrants_action` → **true**. Reasoning tier unlocked. |
| 00:07.2 | Planner | 3 options. Recommends **rebook via Antwerp, $1,200, saves 62h**. Ran a sandboxed cost simulation. `do_nothing_consequence`: "$15,480 penalty, strategic account at risk." |
| 00:10.3 | Critic | Receives evidence + options only. Verdict **approved**. Finding: "success probability 0.85 assumes Antwerp capacity, not directly evidenced." |
| 00:10.3 | OPA | Tier **T3**. $1,200 > $250 threshold; customer is strategic. → **requires_approval: true**, reasons attached. |
| 00:10.4 | API | Approval card published. Workflow **suspends**. |
| — | *(stack is restarted for a deploy during this window)* | Workflow is unaffected. |
| 04:12:00 | Operator | Opens card. Reads: what happened, 5 evidence refs, 3 options with costs, critic's caveat, $15,480 if nothing done. Approves in 24 seconds. |
| 04:12:00 | API | Signals workflow. Resumes at Stage 6. |
| 04:12:01 | Action Broker | Idempotency clear → policy re-check → persist `proposed` → lock `SHP-10432` → carrier rebook → persist `executed` → audit row with `trace_id`. |
| 04:12:02 | Comms agent | Drafts customer notification. T2, cost $0 → autonomous. |
| — | Result | **$1,200 spent, $15,480 avoided. Total agent cost: $0.41.** |

**What the trace shows afterwards:** one root span, five sibling agent spans proving real concurrency, a sandbox span nested inside the planner, a 4-hour `approval.wait` span, and a final action span linked by `trace_id` to the audit row. Every claim traceable to an `EvidenceRef`.

**What did not happen:** the duplicate event caused nothing. No agent ever held the capability to rebook. The strategic-customer rule forced human review even though cost alone might have been borderline.

---

## 7. Failure paths

Every failure has a defined destination. That is what "failure is a first-class state" means in practice.

| Failure | Behaviour |
|---|---|
| Agent times out | Temporal retries that activity only, 3x with backoff |
| Agent output fails validation | Pydantic AI retries with the error fed back, 2x |
| Agent fails after all retries | Workflow escalates to human with partial assessments attached |
| Budget exhausted | `BudgetExceeded`, non-retryable → escalate with partials |
| Critic rejects everything | Escalate; never auto-act on a rejected plan |
| Approval times out | Escalate to a wider group; never silently act |
| Broker executor fails | Compensate, mark failed, escalate |
| Worker crashes mid-fan-out | Temporal replays; completed activities are **not** re-run |
| Redpanda down | Receiver returns 5xx; carrier retries; nothing lost |
| Postgres down | Ingestor pauses, consumer lag alerts, no data lost |
| LiteLLM budget exceeded | Agent calls fail fast → escalate. Detection keeps working. |
| Injection in payload | Agent may be misled; **no action possible** — no capability |

Notice the pattern: **every path ends in either a retry or a human, never in a silent drop and never in an unreviewed action.** An escalation is a successful outcome of the failure-handling system, not a failure of the system.

The last row is the one to internalize. Prompt defences are mitigations that reduce how often an agent is fooled. Capability separation is the boundary that makes being fooled survivable. Build both, but only rely on the second.

---

## 8. Data flow summary

| Store | Holds | Truth? |
|---|---|---|
| `shipment.events.raw` | Original carrier payloads | Yes — replayable |
| `shipment_events` | Normalized, deduplicated archive | Yes — the system of record |
| `shipments` | Current state projection | No — rebuildable from events |
| Workflow history | Process state per shipment/exception | Yes — for process |
| `action_audit` | Every effect on the world | Yes — compliance artifact |
| Langfuse traces | Agent reasoning, cost, latency | Yes — for behaviour |
| `shipment_exceptions` | Detected exceptions | Derived |

Two sources of truth by design, cleanly separated: **the event log holds facts about the world; the workflow history holds facts about our process.** Conflating them is the most common architectural mistake in this domain — you end up either running long-lived state machines inside Kafka consumers or streaming high-volume telemetry through workflow histories, and both are miserable.

`shipments` being a rebuildable projection is what makes schema evolution safe: change the projection, replay the archive, done.

---

**Next:** [07-SCALING-PATH.md](./07-SCALING-PATH.md) — what breaks first, and what to do about it.
