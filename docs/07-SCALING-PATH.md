# 07 — Scaling Path

**Goal of this document:** know what breaks first, in what order, and what the fix is — so that when it happens you are executing a plan rather than improvising during an incident.

---

## 1. What breaks first

Scaling is not uniform. Components fail in a predictable order, and it is rarely the one people expect.

| Order | Breaks at roughly | Symptom | Fix |
|---|---|---|---|
| 1 | **Any volume** | Model spend grows faster than exception count | Detection gate is leaking to the reasoning tier |
| 2 | ~1k events/day | Approval queue depth climbs, operators stop reading | Raise autonomy thresholds; fix false alarm rate |
| 3 | ~10k events/day | Ingestor consumer lag climbs | Scale ingestor replicas |
| 4 | ~50k events/day | Agent activity latency rises | Scale worker fleet; separate task queues |
| 5 | ~100k events/day | Postgres write contention on `shipment_events` | Partition, compress, offload analytics |
| 6 | ~500k events/day | Detection latency SLO breached | Move detection to a stream processor |
| 7 | ~1M events/day | Temporal history service saturates | Dedicated cluster, shard by namespace |

**The first row is not a scale problem — it is an architecture problem that merely becomes visible under scale.** It is also the most common one. Watch the ratio of `agent_cost_usd_total{model_tier="reasoning"}` to `exceptions_detected_total` from day one and keep it flat. If reasoning-tier calls track event volume rather than exception volume, no amount of infrastructure will save the unit economics.

**The second row is not a technical problem at all**, and it is the one that actually kills deployments. An operator who is paged for eleven non-issues stops reading the twelfth. At that point recall is irrelevant because nobody is looking. Fix the false alarm rate before you scale anything.

---

## 2. Layer-by-layer migration

### Compute platform: Compose → Kubernetes

**Trigger:** you need more than one machine, or you need zero-downtime deploys.

The topology is already correct — Compose and Kubernetes describe the same service graph. What changes is scheduling, not architecture.

| Compose | Kubernetes |
|---|---|
| `docker compose up` | Helm chart per service |
| `depends_on` | readiness/liveness probes |
| `--network none` | `NetworkPolicy` with default-deny egress |
| Single worker process | HPA on Temporal task queue depth |
| Env vars | External Secrets Operator → Vault/Infisical |
| Docker sandbox runner | Jobs with `runtimeClassName: gvisor` |

Do these in order and each is independently useful:

1. Chart the stateless services (api, worker, ingestor).
2. Move stateful services (Postgres, Redpanda) to operators or managed offerings.
3. Add `NetworkPolicy` — this is where the egress rules from [02 §6](./02-SANDBOXING.md) become enforceable rather than aspirational.
4. Switch sandbox execution to Kubernetes Jobs with gVisor.
5. Autoscale workers on Temporal queue depth, not CPU. CPU is a terrible proxy for a fleet that spends its time waiting on model APIs.

That last point is worth emphasizing: an agent worker at 4% CPU can still be completely saturated on concurrent activity slots. Scaling on CPU will under-provision it badly.

---

### Sandbox: Docker → gVisor → Firecracker

**Trigger for gVisor:** anything that is not your own laptop. **Trigger for microVM:** multi-tenant, or genuinely hostile input.

Because the isolation primitive was factored into `SandboxProfile.runtime` in Phase 2, this is a configuration change:

```python
SIMULATION = replace(SIMULATION, runtime="runsc")   # gVisor
```

```yaml
# Kubernetes
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: { name: gvisor }
handler: runsc
```

For Tier 3, **E2B self-hosted** is the intended target: Firecracker microVMs, Apache-2.0, Terraform-provisioned, dedicated guest kernel per session. Kata Containers on your own cluster is the alternative if you would rather not add a platform.

Do **not** revisit Daytona. It went closed-source in June 2026; the public repository is frozen at its last open release and receives no security patches.

The cost of each tier is roughly 50ms → 150ms → 150–500ms of cold start. For a workload where the model call takes seconds, that is noise. **There is no performance argument for staying on weak isolation in production.**

---

### Event bus: single Redpanda → cluster

**Trigger:** more than ~50k events/day, or you cannot tolerate losing the broker.

1. 3 brokers, `replication.factor=3`, `min.insync.replicas=2`.
2. Increase partitions on `shipment.events.raw` — but note that partition count is effectively **your maximum ingestor parallelism**, so size it above your projected peak. Increasing later rebalances keys and disrupts per-shipment ordering guarantees during the transition.
3. Tiered storage to S3 for long retention. This directly serves the eval harness: replaying a real incident from six months ago is only possible if the events still exist.
4. Migrating to Apache Kafka, if ever needed, is a connection-string change. That was the point of choosing a protocol rather than an implementation.

---

### Detection: Python consumer → Apache Flink

**Trigger:** detection latency p95 breaches 10 seconds, or windowed rules become awkward in application code.

This is the deferral from [00-ARCHITECTURE.md §7](./00-ARCHITECTURE.md) coming due. Flink's genuine value in this domain is not throughput — it is **event-time processing with watermarks**, handling late and out-of-order events correctly as a first-class concern rather than as application logic you maintain by hand.

```sql
-- The rule that is painful in application code and natural in Flink SQL
CREATE TABLE shipment_events (
    shipment_id STRING,
    event_time  TIMESTAMP(3),
    status      STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '2' MINUTE
) WITH ('connector' = 'kafka', ...);

SELECT shipment_id,
       TUMBLE_START(event_time, INTERVAL '6' HOUR) AS window_start,
       COUNT(*) AS scans
FROM shipment_events
GROUP BY shipment_id, TUMBLE(event_time, INTERVAL '6' HOUR)
HAVING COUNT(*) = 0;   -- silence detection, correct under late arrival
```

Watermark tuning is the whole game: too short and you miss late events, too long and your real-time alerts are not real-time. **Two minutes is a sane starting point for carrier data**; validate it against the observed `ingest_lag` distribution rather than guessing.

Migrate incrementally — move one rule at a time, run both implementations in parallel, and compare outputs on the golden suite before cutting over. A detection layer is not a thing to switch over in one deploy.

---

### ETA prediction: heuristic → trained model

**Trigger:** you have 6+ months of real outcomes.

The interface was designed for this in [05 §7](./05-DOMAIN-MOCK-INGESTION.md):

```python
predictor: ETAPredictor = LightGBMETAPredictor(model_path=...)
```

Features that actually matter, roughly in order: lane historical delay distribution, carrier on-time performance by lane, current port congestion, weather severity, day of week and seasonality, customs clearance history by destination, and time already spent in current status.

Run the heuristic and the model side by side against the golden suite first. **Ship the model only when it demonstrably wins.** A trained model that is worse than your heuristic is a common and embarrassing outcome, and the eval harness exists precisely so you find out before deploying rather than after.

---

### Models: cost optimization

At scale, model spend dominates infrastructure spend, often by an order of magnitude. Three levers, in descending order of impact:

1. **Tighten the detection gate.** Discussed above. Nothing else comes close.
2. **Self-host the fast tier.** The high-volume classification and normalization work is well within reach of an open model on vLLM. Migrate one agent at a time, validated by evals. Signal Interpreter is usually the highest-volume and therefore the first candidate.
3. **Cache aggressively.** Disruption context for the same port within a 15-minute window is the same answer. LiteLLM's Redis cache handles this; it is a configuration change, not an application change.

Order matters. Teams commonly reach for self-hosting first because it feels like the serious engineering move, when the detection gate would have delivered a larger saving for a fraction of the effort.

---

### Temporal: auto-setup → production cluster

**Trigger:** anything beyond development. `auto-setup` is explicitly not a production deployment.

1. Run frontend, history, matching, and worker services separately.
2. Dedicated Postgres or Cassandra for persistence — not shared with application data.
3. Elasticsearch for advanced visibility if you need rich workflow search.
4. Separate namespaces per environment and per tenant, with distinct retention.
5. Archival to S3 for closed workflows.

Temporal Cloud is a legitimate answer if you would rather not operate this. The workflow code does not change either way, which is the property that makes the decision reversible and therefore low-stakes.

---

### Postgres: single → partitioned + offloaded

**Trigger:** write contention on `shipment_events`, or analytical queries interfering with operational ones.

1. Timescale compression policy on `shipment_events` older than 30 days — typically 10–20x on this shape of data.
2. Continuous aggregates for dashboard queries so Grafana stops scanning raw hypertables.
3. Read replicas for the query API and the UI.
4. CDC via Debezium into ClickHouse for analytics. **This is the right moment to add a second database and not before** — the workload has genuinely diverged, which is the only good reason to split a store.
5. Partition `action_audit` by month; it grows forever by design.

---

## 3. Multi-tenancy

**Trigger:** the first external customer.

Deferred in the POC because every abstraction it forces is cheap to add later and expensive to carry early. When it arrives:

| Layer | Approach |
|---|---|
| Data | `tenant_id` on every table + row-level security in Postgres |
| Events | `tenant_id` in the key prefix; consider dedicated topics for large tenants |
| Workflows | Temporal namespace per tenant, or `tenant_id` in the workflow id |
| Policy | OPA `data.tenants[id].thresholds` — per-tenant autonomy settings |
| Models | LiteLLM team budgets per tenant |
| Traces | `tenant_id` as a Langfuse trace attribute |
| Sandbox | Never share a sandbox across tenants; this is where microVM isolation stops being optional |

The autonomy layer is where tenancy is most visible to customers, and it is a genuine product feature: an enterprise account will want approval on everything, a small account will want none. Because those thresholds live in OPA data rather than code, this is configuration.

---

## 4. Reliability targets

| Component | Target | Mechanism |
|---|---|---|
| Webhook receiver | 99.95% | Stateless, multi-replica, carrier retries absorb the rest |
| Event durability | No loss | RF=3, `min.insync.replicas=2`, ack=all |
| Workflow durability | No loss | Temporal persistence + archival |
| Action exactly-once | Guaranteed | Idempotency key + unique constraint |
| Detection availability | 99.9% | Multiple consumer replicas |
| Agent availability | 99% | Degrades to human escalation, not to failure |

That last row is a deliberate design position. **Agents are allowed to be the least reliable component**, because their failure mode is escalation to a human rather than data loss or a wrong action. Everything below them in the stack is held to a higher standard. This is what makes it acceptable to build on a non-deterministic worker at all.

---

## 5. Cost model at scale

Illustrative, at 100k events/day and a 2% exception rate:

| Item | Monthly | Note |
|---|---|---|
| Fast-tier model calls | $400 | 2,000 exceptions/day × 5 agents |
| Reasoning-tier calls | $900 | ~40% of exceptions reach the gate |
| Compute (k8s, ~12 nodes) | $1,800 | |
| Postgres (managed, HA) | $600 | |
| Redpanda cluster | $500 | |
| Temporal Cloud | $400 | Or self-hosted compute |
| Observability | $200 | Self-hosted Langfuse |
| **Total** | **~$4,800** | ~$0.08 per exception |

Compare against value: at 2,000 exceptions/day with even a modest average avoided penalty, the return is not close. **But that entire argument collapses if the false alarm rate is high**, because then you are paying for exceptions that were not exceptions and burning operator attention on top. The economics of this system are downstream of detection precision, not of infrastructure cost.

Two sensitivities worth watching: if reasoning-tier share rises from 40% to 80%, model cost roughly doubles — check the gate. If it rises because event volume rose rather than exception rate, the gate has failed entirely.

---

## 6. Migration sequencing

Do these in order. Each is independently valuable and none blocks the others unnecessarily.

**Immediately after the POC**
1. Pin every image to a digest
2. Real secrets management (Vault/Infisical), no `.env` in production
3. Temporal production topology
4. gVisor for all sandbox execution
5. CI eval gate on every change to agents, prompts, or policy

**First 3 months of production**
6. Kubernetes with NetworkPolicy
7. Redpanda cluster with tiered storage
8. Postgres HA + read replicas
9. Real carrier adapters, one at a time, each with recorded golden scenarios
10. On-call runbooks driven by the Phase 4 alerts

**As scale demands**
11. Flink for detection
12. Trained ETA model
13. Self-hosted fast tier
14. Multi-tenancy
15. ClickHouse for analytics

Note that items 1–5 are all things that could be done in a week and dramatically change the risk profile. Do not skip them because production traffic feels more urgent — production traffic is exactly why they matter.

---

## 7. What stays the same

Worth stating explicitly, because it is the return on the architectural discipline of Phases 1–3.

Through every migration above, these do not change:

- The five invariants
- The canonical event schema
- Agent output contracts
- The workflow structure — fan out, plan, critique, gate, act
- Capability separation between agents and the Action Broker
- OPA autonomy tiers
- The evaluation harness and its scenario format

**You are replacing implementations behind stable interfaces.** That is the difference between a system that scales and one that gets rewritten — and it is why Phase 1 spent so much time on verdicts, replaceability, and where the lock-in actually sits.

---

## 8. What to reconsider annually

Not everything should be defended forever. The layers below move fast enough that an annual re-evaluation is honest engineering rather than churn:

| Layer | Why revisit | Stable? |
|---|---|---|
| Agent framework | Fastest-moving layer in the stack | No — keep it thin, keep the contracts |
| Models | New capability tiers and price points constantly | No — re-benchmark against your evals |
| Sandbox platform | Active competition, vendors change licenses | Maybe — Daytona is the cautionary tale |
| Observability backend | OTel means the backend is an env var | Backend no, **standard yes** |
| Orchestration | Slow-moving, high switching cost | Yes |
| Event bus | Protocol is the standard | Yes |
| Postgres | Boring on purpose | Yes |

The pattern to carry forward, and arguably the most transferable lesson in this entire guide:

> **Invest heavily in the slow-moving layers. Keep the fast-moving layers thin and behind interfaces you own.**

Your durable orchestration, your event schema, your policy model, and your evaluation harness should outlive several generations of agent frameworks and models. If a model upgrade requires touching more than prompts and configuration, the boundary is in the wrong place.

---

**Back to:** [README.md](./README.md)
