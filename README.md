# AIOps Shipment Monitoring System — Implementation Guide

An infrastructure-first implementation guide for building a **production-shaped, multi-agent AIOps platform**, using multi-carrier shipment monitoring as the running domain.

---

## What this actually is

Read this before anything else, because it determines every decision downstream.

**The goal is the AIOps platform. Shipment monitoring is the payload.**

We are not building a logistics product. We are building the infrastructure that lets many specialized AI agents run in parallel, coordinate through durable orchestration, execute untrusted work inside sandboxes, request human approval for consequential actions, and be observed, evaluated, and cost-controlled like any other production system.

Shipment monitoring was chosen as the domain because it exercises every part of that platform honestly:

| Platform capability | Why shipment monitoring exercises it |
|---|---|
| Parallel multi-agent fan-out | Weather, port congestion, vessel position, customs, carrier signals are genuinely independent reads |
| Long-running durable orchestration | A shipment lifecycle spans weeks; workflows must survive restarts |
| Human-in-the-loop approval | Rebooking and expediting cost real money |
| Serialized write path | Two agents rebooking the same shipment is a real, expensive failure |
| Sandboxing | Untrusted carrier payloads, document parsing, what-if simulation code |
| Evaluation | Historical shipments give you objective ground truth to replay against |

Because the platform is the product, the **ingestion layer is deliberately mocked**. There are no carrier API keys, no EDI VANs, no vendor contracts. A mock carrier service emits synthetic but realistic event streams, including the messy parts (out-of-order events, duplicates, silence, ambiguous exception codes). This lets you build and test the entire AIOps stack today, and swap in real carriers later behind one adapter interface.

---

## Build order (this order is not optional)

The single most common way projects like this fail is starting with agents. You end up with impressive demos sitting on infrastructure that cannot restart, cannot be traced, cannot be evaluated, and cannot be trusted with a write.

We build bottom-up:

```
Phase 1  Infra foundation      →  event bus, state, durable orchestration, gateway, policy
Phase 2  Sandboxing            →  isolation boundary + capability separation
Phase 3  Agents + orchestration→  typed agents, parallel fan-out, HITL gates, action broker
Phase 4  AIOps layer           →  tracing, metrics, evals, cost guardrails
Phase 5  Domain integration    →  mock ingestion, canonical schema, scenarios
Phase 6  Control tower UI      →  exception queue, approval cards, trace drill-down
```

Every phase ends with a **checkpoint** you can verify before moving on.

---

## Documents

All implementation guides live in [`docs/`](./docs/).

| # | Document | What it covers |
|---|---|---|
| 00 | [00-ARCHITECTURE.md](./docs/00-ARCHITECTURE.md) | Mental model, the five design invariants, target architecture, repo layout |
| 01 | [01-INFRA-FOUNDATION.md](./docs/01-INFRA-FOUNDATION.md) | Phase 1 — every infra component with an explicit verdict and rejected alternatives |
| 02 | [02-SANDBOXING.md](./docs/02-SANDBOXING.md) | Phase 2 — threat model, sandbox runner, capability separation |
| 03 | [03-AGENTS-AND-ORCHESTRATION.md](./docs/03-AGENTS-AND-ORCHESTRATION.md) | Phase 3 — agent contracts, Temporal workflows, parallel fan-out, approval gates |
| 04 | [04-AIOPS-OBSERVABILITY-EVALS.md](./docs/04-AIOPS-OBSERVABILITY-EVALS.md) | Phase 4 — OTel GenAI tracing, SLOs, replay evaluation harness, cost control |
| 05 | [05-DOMAIN-MOCK-INGESTION.md](./docs/05-DOMAIN-MOCK-INGESTION.md) | Phase 5 — canonical event schema, mock carrier service, scenario generator |
| 06 | [06-SYSTEM-FLOW.md](./docs/06-SYSTEM-FLOW.md) | End-to-end walkthrough of one exception, from event to resolved action |
| 07 | [07-SCALING-PATH.md](./docs/07-SCALING-PATH.md) | POC → production migration for each layer, and what breaks first |

Read 00 first. Then follow 01 through 05 in order. Document 06 is the one to re-read whenever you lose the thread of how the pieces connect.

---

## Quickstart

```bash
# 1. Bring up the core substrate
make infra-core

# 2. Bring up the observability plane
make infra-obs

# 3. Verify every component is healthy
make doctor

# 4. Start the Temporal worker (hosts agents + activities)
make worker

# 5. Start the API + mock carrier feed
make api
make mock-feed SCENARIO=port_congestion

# 6. Open the consoles
#   Control tower      http://localhost:3002
#   Temporal UI        http://localhost:8233
#   Langfuse           http://localhost:3000
#   Redpanda Console   http://localhost:8080
#   Grafana            http://localhost:3001
```

---

## Definition of done

The POC is complete when all of the following are true:

- [ ] A synthetic disruption injected into the mock feed produces a scored exception within 10 seconds
- [ ] Five read-agents fan out in parallel and their spans appear as siblings in one Langfuse trace
- [ ] A T3 action pauses the workflow on a durable signal and survives a full `docker compose restart`
- [ ] The Action Broker rejects a duplicate action via idempotency key, provably, in the audit log
- [ ] `make eval` replays 50 golden scenarios and prints precision, recall, lead time, and cost per exception
- [ ] A per-exception cost cap terminates a runaway workflow and escalates to a human
- [ ] Sandboxed simulation code cannot reach the network or the host filesystem, proven by a failing test

If you can tick all seven, you have an AIOps platform. Swapping the mock carrier for DHL, FedEx, and Maersk is then an adapter problem, not an architecture problem.
