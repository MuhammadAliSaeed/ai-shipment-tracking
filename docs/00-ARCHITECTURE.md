# 00 — Architecture and Mental Model

---

## 1. The mental model

> **An AIOps platform is a distributed system whose workers are unreliable, non-deterministic, and expensive. Everything you build is a strategy for containing those three properties.**

An LLM agent is a worker that:

- **Fails non-deterministically.** The same input can produce different output. Retries are not free and not idempotent by default.
- **Cannot be trusted with a write.** It is a confused deputy. Anything it reads — a carrier payload, a news snippet, a PDF — can carry instructions.
- **Costs money per unit of thought.** Unlike a normal worker, an infinite loop has a bill attached.

Every architectural decision in this guide traces back to containing one of those three. If a decision does not, it is decoration.

The corollary that matters most: **the quality ceiling of this system is not set by the model. It is set by the deterministic layer surrounding it.** Two teams using the identical model will get wildly different results depending on the quality of their schemas, their rules engine, their evaluation harness, and their action boundary. Spend your effort there.

---

## 2. The five design invariants

Everything in this guide is an implementation of one of these. When you are unsure about a design choice, check it against this list.

### Invariant 1 — Every unit of work is resumable

A process crash, a deploy, or a node eviction must never lose work. A shipment workflow that has been running for three weeks and is waiting on a human approval must survive a restart of every service in the stack.

*Implemented by:* Temporal durable workflows. Nothing important lives in process memory.

### Invariant 2 — Every unit of work is observable

If you cannot reconstruct exactly what an agent saw, what it decided, what it cost, and why, you cannot improve it. In agentic systems this is literally true, not aspirational — behaviour is not readable from the source code.

*Implemented by:* OpenTelemetry GenAI semantic conventions on every span, exported to Langfuse. One trace per exception, spans for every agent, tool call, and sandbox execution.

### Invariant 3 — Every unit of work is bounded

Bounded in iterations, tokens, wall-clock time, and money. An unbounded agent loop is an outage with an invoice.

*Implemented by:* Per-activity timeouts and retry policies in Temporal, per-exception token/cost budget enforced in workflow code, hard kill on breach with escalation to a human.

### Invariant 4 — Reads fan out, writes serialize

Parallel reads are where multi-agent architectures genuinely pay for themselves — independent signals, aggregated. Parallel writes are where they fail, because two agents acting on the same entity carry conflicting implicit assumptions.

*Implemented by:* Parallel agent activities for signal gathering; a single non-LLM **Action Broker** with per-shipment serialization and idempotency keys for every state-changing effect.

### Invariant 5 — Agents propose, the platform disposes

An agent never holds a capability. It holds the ability to *request* that a capability be exercised. The thing that actually calls the carrier API, sends the email, or spends the money is deterministic code that validates against policy first.

*Implemented by:* Capability separation between agent activities and the Action Broker, with OPA policy as the gate.

---

## 3. Where AI is used, and where it is banned

This table is the single most important design artifact in the project. Get it wrong and you will have a system that is slow, expensive, and less accurate than a spreadsheet.

| Concern | Implementation | LLM? |
|---|---|---|
| Carrier payload → canonical schema | Deterministic mapper per carrier | **No** |
| Duplicate / out-of-order event handling | Event-time processing, idempotency keys | **No** |
| SLA breach detection | Rules engine (OPA / SQL) | **No** |
| ETA / delay prediction (numeric) | Gradient boosting (LightGBM); heuristic in POC | **No** |
| Anomaly detection on telemetry | Statistical baselines | **No** |
| Reroute cost optimization | OR-Tools | **No** |
| Autonomy tier / approval decision | OPA policy | **No** |
| Action execution | Action Broker | **No** |
| Ambiguous carrier exception code → meaning | Signal Interpreter agent | **Yes** |
| Fusing heterogeneous unstructured signals | Context agents | **Yes** |
| Generating remediation options with tradeoffs | Remediation Planner agent | **Yes** |
| Adversarial review of a recommendation | Critic agent | **Yes** |
| Drafting customer / ops communication | Comms agent | **Yes** |

**The rule:** if a deterministic implementation exists, use it. The LLM's job is reasoning over ambiguity and heterogeneity — nothing else. A numeric prediction routed through an LLM is a bug, not a feature.

---

## 4. Target architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          CONTROL TOWER UI (Next.js)                      │
│      exception queue · approval cards · shipment timeline · traces       │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ REST + SSE
┌───────────────────────────────┴──────────────────────────────────────────┐
│                            API (FastAPI)                                 │
│         webhook receiver · query · approval submit · admin               │
└───────┬──────────────────────────────────────────────┬───────────────────┘
        │                                              │
        │ produce                                      │ signal
┌───────▼────────────────┐                  ┌──────────▼───────────────────┐
│   EVENT BUS (Redpanda) │                  │  ORCHESTRATION (Temporal)    │
│  shipment.events.raw   │                  │                              │
│  shipment.events.norm  │─── consume ─────▶│  ShipmentLifecycleWorkflow   │
│  shipment.exceptions   │                  │      (one per shipment)      │
│  actions.audit         │                  │            │                 │
└───────▲────────────────┘                  │            ▼                 │
        │                                   │  ExceptionResolutionWorkflow │
┌───────┴────────────────┐                  │      (one per exception)     │
│  INGESTION (mocked)    │                  └──────┬───────────────┬───────┘
│  mock carrier service  │                         │               │
│  normalizer            │                         │               │
│  detector (rules)      │              ┌──────────▼─────┐  ┌──────▼───────┐
└────────────────────────┘              │ AGENT ACTIVITIES│  │ HITL GATE    │
                                        │  parallel reads │  │ durable wait │
                                        │  ┌───────────┐  │  │ on signal    │
                                        │  │ Signal    │  │  └──────┬───────┘
                                        │  │ Disruption│  │         │
                                        │  │ Route     │  │         │
                                        │  │ Customs   │  │         │
                                        │  │ Impact    │  │         │
                                        │  └─────┬─────┘  │         │
                                        │        ▼        │         │
                                        │   Remediation   │         │
                                        │        ▼        │         │
                                        │     Critic      │         │
                                        └────────┬────────┘         │
                                                 │ proposal         │ approved
                                                 ▼                  ▼
                                        ┌────────────────────────────────────┐
                                        │  ACTION BROKER (no LLM)            │
                                        │  policy check · idempotency ·      │
                                        │  serialize · execute · audit       │
                                        └────────────────────────────────────┘

  CROSS-CUTTING ────────────────────────────────────────────────────────────
  LiteLLM gateway  ·  OPA policy  ·  Sandbox runner  ·  Postgres/Timescale
  OTel Collector → Langfuse (agent traces) + Prometheus/Grafana (infra)
```

### Reading the diagram

Four things are worth noticing, because they are the design:

1. **The event bus and the orchestrator are separate systems with separate jobs.** Redpanda carries facts (what happened). Temporal carries process (what we are doing about it). Conflating them is the most common architectural mistake in this domain — you end up either doing long-running state machines in Kafka consumers, or streaming high-volume telemetry through workflow histories. Both are painful.

2. **Agents live inside Temporal activities, never inside workflow code.** Workflow code must be deterministic and replayable. An LLM call is the least deterministic thing in the system. This boundary is non-negotiable and Temporal will fail loudly if you cross it.

3. **Every arrow into the Action Broker is a proposal.** No agent has a line to the outside world. This single constraint is what makes the system safe enough to grant autonomy at all.

4. **Observability is not a layer at the bottom, it is a cross-cut.** The trace spans the API, the workflow, every agent, the sandbox, and the broker. If your trace stops at the agent boundary, you will not be able to debug the interesting failures.

---

## 5. Repository layout

```
aiops-shipment-monitoring/
├── Makefile                          # every operation is a make target
├── .env.example
├── pyproject.toml                    # uv workspace
│
├── infra/
│   ├── docker-compose.core.yml       # postgres, redpanda, temporal, litellm, opa
│   ├── docker-compose.obs.yml        # otel, langfuse(+ch,minio,redis), prom, grafana
│   ├── postgres/init/                # schema + timescale hypertables
│   ├── opa/policies/                 # autonomy tiers, action gates
│   ├── litellm/config.yaml           # model routing + budgets
│   ├── otel/collector.yaml
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/
│
├── packages/
│   ├── contracts/                    # Pydantic models — the shared vocabulary
│   │   ├── events.py                 # CanonicalShipmentEvent
│   │   ├── shipment.py               # ShipmentState
│   │   ├── exceptions.py             # ShipmentException
│   │   ├── assessments.py            # agent output types
│   │   └── actions.py                # ActionProposal, ActionResult
│   ├── platform/                     # infra clients, no domain logic
│   │   ├── bus.py                    # Redpanda producer/consumer
│   │   ├── db.py                     # Postgres/Timescale
│   │   ├── llm.py                    # LiteLLM-backed model factory
│   │   ├── policy.py                 # OPA client
│   │   ├── telemetry.py              # OTel setup, GenAI semconv helpers
│   │   └── budget.py                 # token/cost accounting
│   ├── sandbox/
│   │   ├── runner.py                 # hardened container execution
│   │   └── profiles.py               # isolation profiles
│   ├── agents/
│   │   ├── base.py                   # shared agent scaffolding + tracing
│   │   ├── signal_interpreter.py
│   │   ├── disruption_context.py
│   │   ├── route_risk.py
│   │   ├── customs_docs.py
│   │   ├── impact_assessor.py
│   │   ├── remediation_planner.py
│   │   ├── critic.py
│   │   └── comms.py
│   ├── deterministic/                # the non-LLM decision layer
│   │   ├── normalizer.py
│   │   ├── detector.py
│   │   ├── eta.py
│   │   └── optimizer.py
│   ├── orchestrator/
│   │   ├── workflows/
│   │   │   ├── shipment_lifecycle.py
│   │   │   └── exception_resolution.py
│   │   └── activities/
│   │       ├── agent_activities.py
│   │       ├── tool_activities.py
│   │       └── action_activities.py
│   └── action_broker/
│       ├── broker.py
│       ├── executors/                # notify, rebook, expedite, escalate
│       └── audit.py
│
├── services/
│   ├── api/                          # FastAPI
│   ├── worker/                       # Temporal worker entrypoint
│   ├── ingestor/                     # bus consumer → normalize → detect → signal
│   └── mock_carrier/                 # synthetic feed generator
│
├── evals/
│   ├── scenarios/                    # golden JSON scenarios with expected outcomes
│   ├── replay.py                     # backtest harness
│   └── metrics.py
│
├── ui/                               # Next.js control tower
└── tests/
```

### Layout principles

- **`contracts/` is the vocabulary of the entire system.** Every service imports it, nothing in it imports anything else. If two components disagree about what a shipment event is, you have a bug that will surface at 2am.
- **`platform/` contains no domain logic.** It is the portable half of this project — the part you would carry unchanged into a completely different AIOps use case.
- **`deterministic/` exists as a first-class package specifically to make it socially difficult to sneak an LLM into a place it does not belong.** The directory name is a design constraint.
- **`agents/` never imports `action_broker/`.** Enforce this with an import-linter rule in CI. It is Invariant 5 expressed as a build failure.

---

## 6. Technology summary

Full reasoning for each choice, plus rejected alternatives and scale paths, is in [01-INFRA-FOUNDATION.md](./01-INFRA-FOUNDATION.md). This is the summary table.

| Layer | Choice |
|---|---|
| Event bus | Redpanda (Kafka API) |
| State | PostgreSQL 16 + TimescaleDB + pgvector |
| Durable orchestration | Temporal |
| Agent framework | Pydantic AI |
| Model gateway | LiteLLM proxy |
| Policy engine | Open Policy Agent (Rego) |
| Sandbox | Hardened Docker runner → gVisor → Firecracker/Kata |
| Agent tracing | OpenTelemetry GenAI semconv → Langfuse |
| Infra metrics | Prometheus + Grafana |
| API | FastAPI |
| UI | Next.js + MapLibre |
| Packaging | Docker Compose → Helm/Kubernetes |

---

## 7. What we are deliberately not building in the POC

Being explicit about this prevents scope drift, which is what actually kills projects like this.

| Not building | Why | When it becomes real |
|---|---|---|
| Real carrier integrations | Contracts and credentials are a procurement problem, not an engineering one. Mock preserves the full architecture. | After the platform passes its evals |
| Apache Flink | Real value at millions of events/day. At POC volume, Postgres windows and a Python consumer are clearer and debuggable. | When detection latency or volume forces it |
| Trained ETA model | You need historical data you do not have yet. A calibrated heuristic behind the same interface teaches the same lesson. | Once you have 6+ months of real outcomes |
| Multi-tenancy | Every abstraction it forces is cheap to add later and expensive to carry now. | First external customer |
| Kubernetes | Compose gives the same topology with 10% of the operational cost. | Phase 6+, see 07 |

Note the pattern: in each case we keep the **interface** and stub the **implementation**. That is what makes these deferrals safe rather than technical debt.

---

**Next:** [01-INFRA-FOUNDATION.md](./01-INFRA-FOUNDATION.md) — build the substrate.
