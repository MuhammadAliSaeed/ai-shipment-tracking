# 01 — Phase 1: Infrastructure Foundation

**Goal of this phase:** stand up every piece of infrastructure the agents will later depend on, and verify each one in isolation — *before a single agent exists*.

**Why this order:** an agent failure on top of unverified infrastructure is undebuggable. You will not know whether the model was wrong, the message was lost, the workflow did not resume, or the trace never arrived. Verify the substrate first and every later failure has one plausible cause instead of five.

Each component below follows the same structure: **what it does → verdict (why this, what was rejected) → configuration → checkpoint.**

---

## Step 1.0 — Project skeleton

```bash
mkdir -p aiops-shipment-monitoring && cd aiops-shipment-monitoring
git init

# Python toolchain
uv init --python 3.12
uv add pydantic pydantic-settings pydantic-ai temporalio confluent-kafka \
       asyncpg sqlalchemy alembic httpx fastapi uvicorn \
       opentelemetry-sdk opentelemetry-exporter-otlp \
       opentelemetry-instrumentation-fastapi structlog
uv add --dev pytest pytest-asyncio ruff mypy import-linter
```

> **Verdict — `uv` over pip/poetry.** Resolution is fast enough that CI cost stops being a consideration, workspace support handles the multi-package layout natively, and it is a single static binary in the Docker build. Poetry works fine; the migration cost either direction is one afternoon, which is the definition of a low-stakes decision. Do not spend time here.

Create the directory skeleton from [00-ARCHITECTURE.md §5](./00-ARCHITECTURE.md).

Add the architectural constraint as a CI-enforced rule in `pyproject.toml`:

```toml
[tool.importlinter]
root_package = "packages"

[[tool.importlinter.contracts]]
name = "Agents cannot execute actions (Invariant 5)"
type = "forbidden"
source_modules = ["packages.agents"]
forbidden_modules = ["packages.action_broker"]

[[tool.importlinter.contracts]]
name = "Contracts depend on nothing"
type = "forbidden"
source_modules = ["packages.contracts"]
forbidden_modules = ["packages.platform", "packages.agents", "packages.orchestrator"]
```

This is worth doing on day one. Invariant 5 is the kind of rule that erodes silently under deadline pressure unless a build fails.

---

## Step 1.1 — PostgreSQL + TimescaleDB + pgvector

**Role:** canonical shipment state, event archive, action audit log, time-series telemetry, and knowledge retrieval. One database, three access patterns.

### Verdict

**Chosen: PostgreSQL 16 with the TimescaleDB and pgvector extensions.**

The decision here is not "is Postgres good" — it is **how many databases this system needs**, and the answer should be one for as long as possible. Three separate stores (relational + time-series + vector) means three backup strategies, three failure modes, three consistency boundaries, and cross-store joins in application code. Timescale and pgvector are extensions, not separate systems, so you get hypertable compression and vector search inside the same transaction as your shipment state.

| Rejected | Why |
|---|---|
| InfluxDB / dedicated TSDB | Second database, second operational surface. Timescale hypertables handle POC and mid-scale telemetry comfortably. |
| Qdrant / Weaviate | Same objection. The knowledge corpus (SOPs, carrier terms, past incidents) is small; pgvector is sufficient by two orders of magnitude. |
| MongoDB | Carrier events look schemaless but are not — normalizing them into a fixed taxonomy is precisely the work that makes downstream reasoning possible. A schema is the feature. |
| ClickHouse as primary | Excellent for analytics, wrong for transactional shipment state. It does appear later as a Langfuse dependency, which is the right role for it. |

**Scale path:** managed Postgres → read replicas for the query API → Citus or partitioning by tenant → move heavy analytics to ClickHouse via CDC. The application code does not change for any of these.

### Configuration

`infra/postgres/init/01-extensions.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

`infra/postgres/init/02-schema.sql`:

```sql
-- Canonical current state of a shipment (mutable projection)
CREATE TABLE shipments (
    shipment_id      TEXT PRIMARY KEY,
    carrier          TEXT NOT NULL,
    mode             TEXT NOT NULL,           -- parcel | ltl | ocean | air
    origin           TEXT NOT NULL,
    destination      TEXT NOT NULL,
    status           TEXT NOT NULL,
    promised_at      TIMESTAMPTZ NOT NULL,
    predicted_eta    TIMESTAMPTZ,
    sla_tier         TEXT NOT NULL DEFAULT 'standard',
    penalty_per_hour NUMERIC(12,2) DEFAULT 0,
    customer_id      TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Immutable event archive. Append-only, this is the source of truth.
CREATE TABLE shipment_events (
    event_id       UUID NOT NULL DEFAULT uuid_generate_v4(),
    shipment_id    TEXT NOT NULL,
    carrier        TEXT NOT NULL,
    event_time     TIMESTAMPTZ NOT NULL,      -- when it happened
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when we learned
    status         TEXT NOT NULL,             -- canonical taxonomy
    raw_status     TEXT,                      -- carrier's original string, always kept
    location       TEXT,
    payload        JSONB NOT NULL,
    idempotency_key TEXT NOT NULL,
    PRIMARY KEY (event_id, event_time)
);
SELECT create_hypertable('shipment_events', 'event_time', if_not_exists => TRUE);
CREATE UNIQUE INDEX ON shipment_events (idempotency_key, event_time);
CREATE INDEX ON shipment_events (shipment_id, event_time DESC);

-- Detected exceptions
CREATE TABLE shipment_exceptions (
    exception_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shipment_id   TEXT NOT NULL REFERENCES shipments(shipment_id),
    kind          TEXT NOT NULL,              -- delay | customs_hold | route_deviation | silence | temp_excursion
    severity      TEXT NOT NULL,              -- low | medium | high | critical
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    detected_by   TEXT NOT NULL,              -- always a deterministic rule id
    evidence      JSONB NOT NULL,
    workflow_id   TEXT,
    state         TEXT NOT NULL DEFAULT 'open',
    resolved_at   TIMESTAMPTZ
);

-- Every state-changing effect the system has ever had on the world
CREATE TABLE action_audit (
    action_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    exception_id    UUID REFERENCES shipment_exceptions(exception_id),
    shipment_id     TEXT NOT NULL,
    action_type     TEXT NOT NULL,
    tier            TEXT NOT NULL,            -- T0..T4
    idempotency_key TEXT NOT NULL UNIQUE,     -- the duplicate-write guard
    proposed_by     TEXT NOT NULL,            -- agent id + version
    policy_decision JSONB NOT NULL,
    approved_by     TEXT,                     -- human id, NULL if autonomous
    status          TEXT NOT NULL,            -- proposed | approved | rejected | executed | failed | compensated
    request         JSONB NOT NULL,
    result          JSONB,
    trace_id        TEXT,                     -- ties this row to the Langfuse trace
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    executed_at     TIMESTAMPTZ
);

-- Positional / sensor telemetry
CREATE TABLE telemetry (
    shipment_id TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    temp_c      DOUBLE PRECISION,
    source      TEXT NOT NULL
);
SELECT create_hypertable('telemetry', 'ts', if_not_exists => TRUE);

-- Operational knowledge for retrieval
CREATE TABLE knowledge_chunks (
    chunk_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    doc_type  TEXT NOT NULL,                  -- sop | carrier_contract | past_incident
    title     TEXT NOT NULL,
    content   TEXT NOT NULL,
    embedding VECTOR(1024),
    metadata  JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX ON knowledge_chunks USING hnsw (embedding vector_cosine_ops);
```

Two schema details carry real design weight:

- **`event_time` vs `ingested_at` are separate columns.** This is the single most important line in the schema. Carrier events arrive late, out of order, and duplicated — a scan that happened at 09:00 can arrive at 09:47. Every window, every rule, and every eval must reason in *event time*. Systems that conflate these produce alerts that are subtly, permanently wrong.
- **`idempotency_key` is unique on both events and actions.** This is how duplicate delivery becomes a non-event rather than a duplicate rebooking.

### Checkpoint

```bash
docker compose -f infra/docker-compose.core.yml up -d postgres
psql "$DATABASE_URL" -c "SELECT extname FROM pg_extension;"   # timescaledb, vector present
psql "$DATABASE_URL" -c "\dt"                                  # six tables
```

---

## Step 1.2 — Redpanda (event bus)

**Role:** the append-only spine. Every fact about the world enters here first. Decouples ingestion from processing, gives replay for free, and makes the evaluation harness possible.

### Verdict

**Chosen: Redpanda, speaking the Kafka API.**

Kafka is unambiguously the correct *protocol* for this domain — event sourcing plus replay is exactly what supply chain visibility needs, and it is what FourKites and similar platforms actually run at scale. The question is only which implementation to operate. Redpanda is a single Go binary with no JVM and no ZooKeeper/KRaft quorum to reason about, starts in about a second, and uses a fraction of the memory in a laptop-sized Compose stack.

The lock-in question — the one that should decide every infrastructure choice — has a clean answer here: **it is the Kafka wire protocol.** Your client library, your topic design, your consumer group semantics, and your offsets are all identical. Migrating to Apache Kafka later is a connection string change. That makes this a genuinely reversible decision, which is why it is safe to optimize for developer experience.

| Rejected | Why |
|---|---|
| Apache Kafka (KRaft) | Correct at scale, but JVM heap tuning and a heavier local footprint buy nothing at POC. Protocol-compatible, so switch anytime. |
| NATS JetStream | Excellent and lighter still, but weaker replay-from-offset ergonomics and a smaller connector ecosystem. Replay *is* our eval harness, so this matters. |
| RabbitMQ | A queue, not a log. Once consumed, the message is gone. Fatal for backtesting. |
| Postgres-only (LISTEN/NOTIFY or a table queue) | Genuinely viable at POC scale, and if you want to cut one component this is the one to cut. Rejected because event-stream thinking is a core lesson here, and retrofitting it later reshapes the whole ingestion path. |

**Scale path:** single broker → 3-broker cluster with RF=3 → tiered storage to S3 for long retention → partition by `shipment_id` for ordering guarantees per shipment (already the design).

### Topic design

```bash
rpk topic create shipment.events.raw        --partitions 6  --replicas 1
rpk topic create shipment.events.normalized --partitions 6  --replicas 1
rpk topic create shipment.exceptions        --partitions 3  --replicas 1
rpk topic create actions.audit              --partitions 3  --replicas 1 \
    --config cleanup.policy=compact,delete --config retention.ms=-1
```

**Partition key is `shipment_id` on every topic, without exception.** Kafka guarantees ordering within a partition, so keying by shipment gives you per-shipment ordering — which is precisely the granularity at which order matters and precisely the granularity at which serialization is required (Invariant 4). This one line of design does a large amount of correctness work for free.

`actions.audit` retains forever and never gets deleted. It is a compliance artifact.

### Checkpoint

```bash
rpk cluster info
rpk topic produce shipment.events.raw --key TEST-1 <<< '{"hello":"world"}'
rpk topic consume shipment.events.raw --num 1 --offset start
# Redpanda Console at http://localhost:8080 shows the message
```

---

## Step 1.3 — Temporal (durable orchestration)

**Role:** owns all control flow, all long-lived state, all retries, all timeouts, and all human-approval waits. This is the backbone of Invariants 1 and 3.

### Verdict

**Chosen: Temporal.**

In the previous architectural discussion the general advice was to start with the lightest durable-execution option (DBOS or Restate) and graduate to Temporal only under scale pressure. **This domain overturns that advice, and it is worth understanding why**, because the reasoning generalizes.

A shipment workflow is not a request that takes 30 seconds. It is an entity that lives for weeks, sleeps on durable timers measured in days, wakes on signals from webhooks, waits indefinitely on human approval, and must exist concurrently in the hundreds of thousands. That is not "durable execution as a nice reliability property" — it is Temporal's canonical, purpose-built use case. Choosing a lighter engine here means reimplementing timers, signals, and workflow queries yourself, which is strictly worse than operating the thing designed for it.

The general rule this illustrates: **pick your durability engine by workflow lifetime and concurrency shape, not by current traffic volume.**

| Rejected | Why |
|---|---|
| DBOS | Minimal footprint is genuinely attractive, but week-long durable timers and signal-driven wakeups at high concurrency are not its center of gravity. |
| Restate | Strong for durable RPC and low-latency service meshes. Our problem is long-lived entities, not chatty inter-service calls. |
| Celery / Arq / RQ | Task queues, not workflow engines. No durable state, no replay, no signals, no human-wait primitive. You would build a bad Temporal. |
| Airflow / Prefect | Batch DAG schedulers. Wrong shape entirely for per-entity, event-driven, long-lived workflows. |
| Hand-rolled state machine in Postgres | The honest fallback, and it works — until you need timers, retries with backoff, replay-safe versioning, and visibility. That is six months of work you can install instead. |

**Scale path:** Compose `auto-setup` → self-hosted cluster with separate frontend/history/matching services on Postgres or Cassandra → Temporal Cloud if you would rather not operate it. Worker fleet scales horizontally and independently of the server.

### Configuration

```yaml
temporal:
  image: temporalio/auto-setup:latest
  depends_on: [postgres]
  environment:
    - DB=postgres12
    - DB_PORT=5432
    - POSTGRES_USER=${POSTGRES_USER}
    - POSTGRES_PWD=${POSTGRES_PASSWORD}
    - POSTGRES_SEEDS=postgres
  ports: ["7233:7233"]

temporal-ui:
  image: temporalio/ui:latest
  depends_on: [temporal]
  environment:
    - TEMPORAL_ADDRESS=temporal:7233
    - TEMPORAL_CORS_ORIGINS=http://localhost:3002
  ports: ["8233:8080"]
```

> `auto-setup` creates its own databases inside your Postgres instance and is intended for development. In production, run the Temporal server components explicitly against a dedicated datastore. See [07-SCALING-PATH.md](./07-SCALING-PATH.md).

Create namespaces up front — separating eval traffic from live traffic from day one saves you from polluting your metrics later:

```bash
temporal operator namespace create --namespace shipment-ops   --retention 30d
temporal operator namespace create --namespace shipment-evals --retention 7d
```

### The one rule you must internalize now

**Workflow code must be deterministic. LLM calls, HTTP calls, database reads, `random`, and `datetime.now()` are all banned inside it.** Every one of those belongs in an activity. Temporal replays workflow code from event history to rebuild state after a crash — if that code is non-deterministic, replay diverges and the workflow breaks in ways that are genuinely unpleasant to debug.

Practically: *workflows decide, activities do.* Since an LLM call is the least deterministic operation in the system, every agent invocation is an activity. This constraint is not friction — it is the same boundary as Invariant 5, arriving from a different direction, and it is a good sign that the architecture is coherent.

### Checkpoint

```bash
temporal operator cluster health
temporal workflow list --namespace shipment-ops   # empty, no error
# Temporal UI at http://localhost:8233
```

---

## Step 1.4 — LiteLLM (model gateway)

**Role:** the single chokepoint through which every token in the system passes. Model routing, per-agent budgets, fallbacks, retries, caching, and unified cost accounting.

### Verdict

**Chosen: LiteLLM proxy, deployed as a service — not as a library import.**

The library and the proxy look interchangeable and are not. As a service it becomes a **policy enforcement point**: budgets and rate limits are enforced for every caller including the ones you did not write, cost accounting is centralized rather than reconstructed from scattered logs, and the model a given agent uses becomes configuration rather than code. Swapping a model becomes a config reload instead of a deploy.

The deeper reason is Invariant 3. A per-exception cost cap that lives in application code is a suggestion. One that lives in a gateway with virtual keys is a control.

| Rejected | Why |
|---|---|
| Direct provider SDKs | Provider lock-in, per-call-site retry logic, and no central place to enforce a budget. |
| LiteLLM as a library | Loses the enforcement point. Fine for a script; wrong for a platform. |
| OpenRouter | Good service, but it is a hosted dependency in the critical path with no self-host story. |
| Portkey / Helicone | Comparable gateways; LiteLLM chosen for permissive licensing, self-hosting, and the broadest provider matrix. |

### Configuration

`infra/litellm/config.yaml`:

```yaml
model_list:
  # High-volume tier: classification, normalization, extraction.
  # These run thousands of times a day. Cost dominates; capability does not.
  - model_name: fast
    litellm_params:
      model: openai/gpt-4o-mini
      api_key: os.environ/OPENAI_API_KEY

  # Reasoning tier: only for exceptions, only after deterministic detection fired.
  - model_name: reasoning
    litellm_params:
      model: anthropic/claude-sonnet-4-5
      api_key: os.environ/ANTHROPIC_API_KEY

  # Local tier: keeps the system runnable with zero spend during development.
  - model_name: local
    litellm_params:
      model: ollama/qwen2.5:14b
      api_base: http://host.docker.internal:11434

router_settings:
  routing_strategy: simple-shuffle
  num_retries: 2
  fallbacks:
    - reasoning: ["fast"]

litellm_settings:
  drop_params: true
  success_callback: ["langfuse"]
  failure_callback: ["langfuse"]
  cache: true
  cache_params:
    type: redis
    host: redis
    ttl: 3600

general_settings:
  master_key: os.environ/LITELLM_MASTER_KEY
  database_url: os.environ/LITELLM_DATABASE_URL
```

Then issue a **virtual key per agent**, each with its own budget:

```bash
curl -X POST http://localhost:4000/key/generate \
  -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
  -d '{"models":["fast"],"max_budget":10,"budget_duration":"1d","key_alias":"signal-interpreter"}'
```

This is what makes cost attribution real. When the bill spikes you will know which agent did it in one query, not after an afternoon of log archaeology.

### Model tiering rule

Route by **volume and ambiguity**, not by importance:

- Every normalized event touches the `fast` tier at most, and ideally touches no model at all.
- The `reasoning` tier is only reachable *after* a deterministic rule has fired. Exceptions are rare; reasoning is expensive; this alignment is what makes the unit economics work.

If your reasoning-tier call count scales linearly with event count, your detection layer is not doing its job.

### Checkpoint

```bash
curl http://localhost:4000/health -H "Authorization: Bearer $LITELLM_MASTER_KEY"
curl http://localhost:4000/v1/chat/completions \
  -H "Authorization: Bearer $AGENT_KEY" \
  -d '{"model":"fast","messages":[{"role":"user","content":"ping"}]}'
curl http://localhost:4000/spend/logs -H "Authorization: Bearer $LITELLM_MASTER_KEY"
```

---

## Step 1.5 — Open Policy Agent (autonomy and action gating)

**Role:** decides what the system is allowed to do without asking a human. This is the mechanism behind *bounded autonomy* — the design principle that the operators actually deploying agents in logistics converged on, and the difference between a system that scales and one that gets switched off after its first expensive mistake.

### Verdict

**Chosen: OPA with Rego policies, as a sidecar service.**

Autonomy rules change constantly and are argued about by people who do not write Python — operations leads, account managers, compliance. If those rules live in application code, every change to "expedite needs approval above $500" is a pull request and a deploy. In OPA they are a hot-reloaded policy file that can be tested, versioned, and diffed independently. Critically, the decision is also *logged with its reasoning*, which is what makes the audit trail defensible.

| Rejected | Why |
|---|---|
| `if` statements in Python | Autonomy logic buried in code, invisible to auditors, redeployed for every threshold tweak. Works for a week. |
| Cerbos | Good tool, oriented toward resource/role authorization. Ours is action-and-cost gating, which Rego expresses more naturally. |
| Casbin | Excellent RBAC/ABAC; not a general policy language. |
| Letting the LLM decide autonomy level | The most dangerous option available. The thing being governed must never be the governor. |

### The autonomy tier model

```rego
package shipment.autonomy

import rego.v1

default tier := "T4"
default requires_approval := true

# T0 — pure observation. Always autonomous.
tier := "T0" if input.action.type in {"enrich", "score", "annotate"}

# T1 — internal notification. No customer contact, no spend.
tier := "T1" if input.action.type in {"internal_alert", "ops_note"}

# T2 — external communication, no money moves.
tier := "T2" if {
    input.action.type in {"customer_notify", "eta_publish"}
    input.action.estimated_cost_usd == 0
}

# T3 — spends money. Autonomous only under a threshold and with confidence.
tier := "T3" if {
    input.action.type in {"rebook", "expedite", "reroute"}
}

# T4 — contractual / financial consequence. Never autonomous.
tier := "T4" if input.action.type in {"claim_file", "penalty_accept", "contract_amend"}

requires_approval := false if tier in {"T0", "T1"}

requires_approval := false if {
    tier == "T2"
    input.agent.confidence >= 0.75
}

requires_approval := false if {
    tier == "T3"
    input.action.estimated_cost_usd <= data.thresholds.auto_spend_usd
    input.agent.confidence >= 0.85
    input.critic.verdict == "approved"
    not input.shipment.customer_tier == "strategic"
}

# Explanations are part of the contract, not a debugging nicety.
reasons contains "cost exceeds autonomous threshold" if {
    tier == "T3"
    input.action.estimated_cost_usd > data.thresholds.auto_spend_usd
}

reasons contains "critic did not approve" if {
    input.critic.verdict != "approved"
}

reasons contains "strategic customer requires human sign-off" if {
    input.shipment.customer_tier == "strategic"
}
```

Note the structure of the T3 autonomous branch: it requires a cost ceiling **and** agent confidence **and** an independent critic approval **and** a customer-tier check. Autonomy is granted by conjunction, never by a single signal. That is the whole idea.

`infra/opa/data/thresholds.json`:

```json
{ "thresholds": { "auto_spend_usd": 250, "max_daily_autonomous_spend_usd": 5000 } }
```

Policies get unit tests, in Rego, running in CI:

```rego
package shipment.autonomy_test

test_expensive_rebook_needs_human if {
    not requires_approval == false with input as {
        "action": {"type": "rebook", "estimated_cost_usd": 900},
        "agent": {"confidence": 0.95},
        "critic": {"verdict": "approved"}
    }
}
```

### Checkpoint

```bash
curl -X POST http://localhost:8181/v1/data/shipment/autonomy \
  -d '{"input":{"action":{"type":"rebook","estimated_cost_usd":100},
                "agent":{"confidence":0.9},
                "critic":{"verdict":"approved"},
                "shipment":{"customer_tier":"standard"}}}'
# => {"result":{"tier":"T3","requires_approval":false,"reasons":[]}}
```

---

## Step 1.6 — Observability plane (OTel → Langfuse + Prometheus)

**Role:** Invariant 2. Detail on instrumentation and evaluation lives in [04-AIOPS-OBSERVABILITY-EVALS.md](./04-AIOPS-OBSERVABILITY-EVALS.md); this step is about standing the plane up before any agent exists so that the first agent you write is traced from its first execution.

### Verdict

**Chosen: OpenTelemetry GenAI semantic conventions as the wire format, Langfuse as the agent-trace backend, Prometheus + Grafana for infrastructure metrics.**

The important part of this decision is the first clause, not the second. Agent observability is no longer a vendor question — it is a specification. Instrumenting to the OTel GenAI conventions (`gen_ai.*` attributes) means Langfuse, Arize Phoenix, Grafana, Datadog, and others all consume the same traces from the same OTLP endpoint. **The backend becomes an environment variable rather than a re-instrumentation project.** Choose the standard, and the vendor choice stops being load-bearing.

Langfuse specifically, because it is open source and self-hostable, it is an OTLP-native backend rather than an SDK you must wrap everything in, and it unifies tracing, prompt versioning, datasets, and evaluation scores in one place — which matters because Phase 4 needs all four connected.

Two planes, deliberately not one: **agent traces and infrastructure metrics answer different questions and have different retention economics.** "Why did this agent recommend a $900 expedite" is a trace question. "Why is consumer lag climbing" is a metrics question. Forcing both into one tool degrades both.

| Rejected | Why |
|---|---|
| LangSmith | Deepest integration with LangChain/LangGraph, which we do not use. Not self-hostable. |
| Arize Phoenix | Strong for eval-heavy notebook workflows; Langfuse fits a long-running service better. Genuinely close call. |
| Logs only | Agent behaviour is not reconstructable from logs. This is the mistake that makes agentic systems undebuggable. |
| Datadog LLM Observability | Fine if already committed to Datadog. Cost and self-hosting rule it out here. |

### Configuration

`infra/otel/collector.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }

processors:
  batch: { timeout: 5s }
  memory_limiter: { check_interval: 1s, limit_mib: 512 }
  attributes/scrub:
    actions:
      - { key: gen_ai.prompt, action: hash }        # enable in prod, not in dev
      - { key: customer.email, action: delete }

exporters:
  otlphttp/langfuse:
    endpoint: http://langfuse:3000/api/public/otel
    headers:
      Authorization: "Basic ${LANGFUSE_OTEL_BASIC_AUTH}"
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlphttp/langfuse]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheus]
```

Set `OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental` in every service. The GenAI conventions are still pre-1.0; opt into the newest attributes deliberately and pin the version you target so an upstream change never surprises you.

### Checkpoint

Emit a hand-rolled span with `gen_ai.*` attributes and confirm it renders in Langfuse as a *generation* with model, tokens, and cost — not as a generic span. If it renders generically, your attribute names are wrong, and fixing that now is far cheaper than after fifty agent call sites exist.

---

## Step 1.7 — Compose files and the Makefile

### `infra/docker-compose.core.yml`

```yaml
name: aiops-core

services:
  postgres:
    image: timescale/timescaledb-ha:pg16
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: shipments
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./postgres/init:/docker-entrypoint-initdb.d
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      retries: 10

  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --overprovisioned
      - --mode dev-container
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    ports: ["19092:19092", "9644:9644"]
    volumes: [redpanda:/var/lib/redpanda/data]
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 10s
      retries: 10

  redpanda-console:
    image: redpandadata/console:latest
    depends_on: [redpanda]
    environment:
      KAFKA_BROKERS: redpanda:9092
    ports: ["8080:8080"]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  temporal:
    image: temporalio/auto-setup:latest
    depends_on:
      postgres: { condition: service_healthy }
    environment:
      DB: postgres12
      DB_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PWD: ${POSTGRES_PASSWORD}
      POSTGRES_SEEDS: postgres
    ports: ["7233:7233"]

  temporal-ui:
    image: temporalio/ui:latest
    depends_on: [temporal]
    environment:
      TEMPORAL_ADDRESS: temporal:7233
      TEMPORAL_CORS_ORIGINS: http://localhost:3002
    ports: ["8233:8080"]

  litellm:
    image: ghcr.io/berriai/litellm:main-latest
    depends_on:
      postgres: { condition: service_healthy }
    command: ["--config", "/app/config.yaml", "--port", "4000"]
    volumes:
      - ./litellm/config.yaml:/app/config.yaml
    environment:
      LITELLM_MASTER_KEY: ${LITELLM_MASTER_KEY}
      LITELLM_DATABASE_URL: ${LITELLM_DATABASE_URL}
      OPENAI_API_KEY: ${OPENAI_API_KEY}
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    ports: ["4000:4000"]

  opa:
    image: openpolicyagent/opa:latest
    command:
      - run
      - --server
      - --addr=0.0.0.0:8181
      - --log-level=info
      - /policies
      - /data
    volumes:
      - ./opa/policies:/policies
      - ./opa/data:/data
    ports: ["8181:8181"]

volumes:
  pgdata:
  redpanda:
```

> **Pin every image to a digest before this leaves your laptop.** `latest` is correct for a guide that must stay readable and wrong for anything reproducible.

### `Makefile`

```makefile
.PHONY: infra-core infra-obs infra-down doctor worker api mock-feed eval

infra-core:
	docker compose -f infra/docker-compose.core.yml up -d
	./scripts/wait-for-health.sh
	./scripts/create-topics.sh
	./scripts/create-namespaces.sh

infra-obs:
	docker compose -f infra/docker-compose.obs.yml up -d

infra-down:
	docker compose -f infra/docker-compose.core.yml -f infra/docker-compose.obs.yml down

doctor:
	uv run python scripts/doctor.py

worker:
	uv run python -m services.worker

api:
	uv run uvicorn services.api.main:app --reload --port 8000

mock-feed:
	uv run python -m services.mock_carrier --scenario $(SCENARIO) --rate $(or $(RATE),5)

eval:
	uv run python -m evals.replay --suite golden --namespace shipment-evals
```

### `scripts/doctor.py`

Write this before you write any agent. It is the highest-leverage 60 lines in the project — it converts "something is broken somewhere" into a named component every single time.

```python
"""Verify every infrastructure dependency in isolation. Run after any infra change."""
import asyncio, sys, httpx, asyncpg
from temporalio.client import Client
from confluent_kafka.admin import AdminClient

CHECKS = []

def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco

@check("postgres + extensions")
async def _pg():
    conn = await asyncpg.connect(DSN)
    exts = {r["extname"] for r in await conn.fetch("SELECT extname FROM pg_extension")}
    assert {"timescaledb", "vector"} <= exts, f"missing extensions: {exts}"
    await conn.close()

@check("redpanda topics")
async def _rp():
    md = AdminClient({"bootstrap.servers": BROKERS}).list_topics(timeout=5)
    required = {"shipment.events.raw", "shipment.events.normalized",
                "shipment.exceptions", "actions.audit"}
    assert required <= set(md.topics), f"missing topics: {required - set(md.topics)}"

@check("temporal namespaces")
async def _tc():
    await Client.connect(TEMPORAL_ADDRESS, namespace="shipment-ops")

@check("litellm completion + spend tracking")
async def _llm():
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{LITELLM_URL}/v1/chat/completions",
                         headers={"Authorization": f"Bearer {AGENT_KEY}"},
                         json={"model": "fast",
                               "messages": [{"role": "user", "content": "ping"}]})
        r.raise_for_status()

@check("opa policy decision")
async def _opa():
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{OPA_URL}/v1/data/shipment/autonomy",
                         json={"input": {"action": {"type": "enrich"}}})
        assert r.json()["result"]["tier"] == "T0"

@check("langfuse otlp ingest")
async def _lf():
    async with httpx.AsyncClient() as c:
        assert (await c.get(f"{LANGFUSE_URL}/api/public/health")).status_code == 200

async def main():
    failed = 0
    for name, fn in CHECKS:
        try:
            await fn()
            print(f"  PASS  {name}")
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            failed += 1
    sys.exit(1 if failed else 0)

asyncio.run(main())
```

---

## Phase 1 checkpoint

Do not proceed until every one of these passes:

- [ ] `make doctor` is green on all six checks
- [ ] Postgres has all six tables plus both hypertables
- [ ] A message produced to `shipment.events.raw` is consumable from offset 0 *after* `docker compose restart redpanda`
- [ ] A trivial "sleep 10 seconds then return" Temporal workflow survives `docker compose restart temporal` and still completes
- [ ] LiteLLM returns a completion and `/spend/logs` shows the cost attributed to the agent's virtual key
- [ ] OPA returns `T3` + `requires_approval: true` for a $900 rebook and `false` for a $100 one
- [ ] A hand-emitted `gen_ai.*` span renders in Langfuse as a generation with token counts

That fourth item is the one people skip. It is the one that proves Invariant 1, and it is the reason the rest of the system can be trusted.

---

**Next:** [02-SANDBOXING.md](./02-SANDBOXING.md) — the isolation boundary.
