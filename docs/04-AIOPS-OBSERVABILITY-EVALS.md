# 04 — Phase 4: The AIOps Layer

**Goal of this phase:** make the system measurable, improvable, and financially bounded. This is the phase that separates an AIOps platform from a demo.

The distinction is worth stating plainly. A demo answers *"can an agent do this?"* An AIOps platform answers *"is it still doing it correctly this week, at what cost, and how would I know if it stopped?"* Everything below exists to answer the second question.

---

## 1. Tracing model

### One trace per exception

Trace boundaries should follow the unit of work a human cares about. Here that is the exception, not the event and not the shipment.

```
TRACE: exception.resolve  (exception_id=exc-7f3a, shipment=SHP-10432)
│
├── SPAN  detect.rules                                  2ms    deterministic
│
├── SPAN  agents.fanout                               1,840ms
│   ├── SPAN  agent.signal_interpreter                1,210ms  gen_ai.*
│   │   └── SPAN  tool.get_event_history                 14ms
│   ├── SPAN  agent.disruption_context                1,780ms  gen_ai.*
│   │   ├── SPAN  tool.get_weather                       82ms
│   │   └── SPAN  tool.get_port_status                   61ms
│   ├── SPAN  agent.route_risk                        1,640ms  gen_ai.*
│   ├── SPAN  agent.customs_docs                        980ms  gen_ai.*
│   └── SPAN  agent.impact_assessor                   1,120ms  gen_ai.*
│
├── SPAN  predict.eta                                    8ms    deterministic
├── SPAN  agent.remediation_planner                   4,320ms  gen_ai.*
│   └── SPAN  sandbox.execute (simulation)              890ms
├── SPAN  agent.critic                                3,110ms  gen_ai.*
├── SPAN  policy.evaluate                                6ms    deterministic
├── SPAN  approval.wait                            8,412,000ms  durable
└── SPAN  action.execute (rebook)                       740ms
    └── SPAN  carrier.api.call                          690ms
```

That tree answers, in one view: which agents ran in parallel (siblings under `agents.fanout`), where the latency actually went, what the sandbox executed, how long the human took, and what the system did to the world. If a stakeholder asks "why did we spend $340 expediting SHP-10432", this is the artifact you open.

Note also how visible the deterministic layer is: rules in 2ms, ETA in 8ms, policy in 6ms. When someone proposes routing one of those through a model, this trace is the argument against it.

### Instrumentation

`packages/platform/telemetry.py`:

```python
def setup_telemetry(service_name: str) -> None:
    resource = Resource.create({
        "service.name": service_name,
        "service.version": settings.version,
        "deployment.environment": settings.env,
    })
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_endpoint))
    )
    trace.set_tracer_provider(provider)


def start_exception_trace(exc: ShipmentException, shipment: ShipmentState):
    """Root span. Attributes here become filterable dimensions in Langfuse."""
    return tracer.start_as_current_span(
        "exception.resolve",
        attributes={
            "session.id": exc.shipment_id,        # groups all traces for a shipment
            "user.id": shipment.customer_id,
            "exception.id": str(exc.exception_id),
            "exception.kind": exc.kind,
            "exception.severity": exc.severity,
            "shipment.carrier": shipment.carrier,
            "shipment.mode": shipment.mode,
            "shipment.sla_tier": shipment.sla_tier,
        },
    )
```

Set `OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental` in every service and pin the convention version you target.

`session.id` and `user.id` are not arbitrary — Langfuse maps those specific attribute names into its session and user model, which is what gives you "show me every exception for this shipment" for free.

### The GenAI attributes that matter

| Attribute | Why |
|---|---|
| `gen_ai.operation.name` | `invoke_agent` vs `chat` — Langfuse renders these differently |
| `gen_ai.agent.name` | Per-agent aggregation of latency, cost, and quality |
| `gen_ai.request.model` | Attribute regressions to a model change |
| `gen_ai.usage.input_tokens` / `output_tokens` | Cost derivation |
| `agent.version` / `agent.prompt_version` | **Attribute regressions to a prompt change** |
| `agent.confidence` | Correlate stated confidence against actual correctness |

The last two are the ones teams add only after their first unexplained regression. Add them now.

---

## 2. Metrics and SLOs

Traces answer *why this one*. Metrics answer *is the fleet healthy*. Two planes, two questions.

### Metrics worth emitting

```python
# Detection quality -- the top of the funnel
exceptions_detected     = Counter("exceptions_detected_total", ["kind", "severity", "carrier"])
exception_lead_time     = Histogram("exception_lead_time_hours", ["kind"])

# Agent behaviour
agent_duration          = Histogram("agent_duration_seconds", ["agent", "model_tier"])
agent_tokens            = Counter("agent_tokens_total", ["agent", "direction"])
agent_cost              = Counter("agent_cost_usd_total", ["agent", "model_tier"])
agent_validation_retry  = Counter("agent_validation_retries_total", ["agent"])

# Autonomy -- the headline number for this platform
actions_proposed        = Counter("actions_proposed_total", ["type", "tier"])
actions_autonomous      = Counter("actions_autonomous_total", ["type", "tier"])
actions_approved        = Counter("actions_human_approved_total", ["type"])
actions_rejected        = Counter("actions_human_rejected_total", ["type"])
approval_latency        = Histogram("approval_latency_seconds", ["tier"])
critic_rejections       = Counter("critic_rejections_total", ["reason"])

# Money
cost_per_exception      = Histogram("cost_per_exception_usd", ["kind"])
budget_exhaustions      = Counter("budget_exhaustions_total", ["stage"])

# Infrastructure
consumer_lag            = Gauge("kafka_consumer_lag", ["topic", "group"])
workflows_open          = Gauge("workflows_open", ["type"])
sandbox_executions      = Counter("sandbox_executions_total", ["profile", "outcome"])
```

### SLOs

Set these before you have data. You will be wrong, and revising a written-down target against evidence is a far better process than inventing one after the fact to match whatever you happen to be achieving.

| SLO | Target | Why this one |
|---|---|---|
| Detection latency (event → exception) | p95 < 10s | Below this, humans perceive it as real-time |
| Resolution latency, autonomous (exception → action) | p95 < 3min | The value proposition versus a human analyst |
| Exception detection recall | > 0.90 | Missed disruptions are the expensive failure |
| False alarm rate | < 0.15 | **The most important number in the system** |
| Autonomous resolution rate | > 0.60 | The actual measure of autonomy |
| Human approval latency | p50 < 5min | If this degrades, your approval cards are bad |
| Cost per exception | < $0.50 | Unit economics |
| Critic rejection rate | 0.05 – 0.20 | A two-sided target — see below |

Two of these deserve explanation.

**False alarm rate is the number that kills these systems in production.** Not accuracy, not latency. An operator who is paged for eleven non-issues stops reading the twelfth, and at that point your recall does not matter because nobody is looking. Weight this above recall in tuning.

**Critic rejection rate is bounded on both sides**, which is unusual and deliberate. Near zero means the critic is rubber-stamping and providing no value — usually because it is receiving the planner's reasoning and being persuaded by it. Above ~0.20 means the planner is generating unsupported recommendations and needs work. The healthy band is a signal that both agents are doing their jobs.

### Alerts

Alert on the things that indicate the *system* is broken, not on individual shipment problems:

```yaml
- alert: DetectionStalled
  expr: rate(exceptions_detected_total[15m]) == 0 and rate(shipment_events_total[15m]) > 0
  annotations:
    summary: "Events flowing but no exceptions detected -- detection layer is down"

- alert: CostPerExceptionSpike
  expr: histogram_quantile(0.95, cost_per_exception_usd) > 2.0
  for: 10m

- alert: AutonomyCollapse
  expr: rate(actions_autonomous_total[1h]) / rate(actions_proposed_total[1h]) < 0.3
  for: 30m
  annotations:
    summary: "Autonomy rate collapsed -- policy change or confidence degradation"

- alert: CriticRubberStamping
  expr: rate(critic_rejections_total[24h]) / rate(actions_proposed_total[24h]) < 0.02
  annotations:
    summary: "Critic approving everything -- check context isolation"

- alert: HumanRejectionSpike
  expr: rate(actions_human_rejected_total[6h]) / rate(actions_human_approved_total[6h]) > 0.4
  annotations:
    summary: "Humans rejecting most recommendations -- quality regression"
```

`DetectionStalled` is the highest-value alert in the list. A monitoring system that silently stops monitoring looks perfectly healthy from the outside — every dashboard is green because nothing is being detected. Always alert on the *ratio* of output to input, never on output alone.

---

## 3. The evaluation harness

This is the most important section in the guide. **It is the shipment-monitoring equivalent of a test suite, and it is what sets the system's quality ceiling.**

### The core idea: replay against ground truth

The domain gives you something rare and valuable: **objective ground truth**. A shipment either was late or it was not. A disruption either materialized or it did not. You do not need an LLM judge to grade the important things.

```
Golden scenario  =  a recorded event stream  +  what actually happened
Evaluation       =  replay the stream, compare the system's decisions to reality
```

This is the analogue of SWE-bench for this domain, and it is the mechanism by which you can safely change a prompt or upgrade a model.

### Scenario format

`evals/scenarios/port_congestion_rotterdam.json`:

```json
{
  "scenario_id": "port_congestion_rotterdam_001",
  "description": "Ocean shipment into Rotterdam during a 4-day congestion event",
  "tags": ["ocean", "port_congestion", "high_value"],

  "shipment": {
    "shipment_id": "SHP-EVAL-001",
    "carrier": "MAERSK",
    "mode": "ocean",
    "origin": "CNSHA",
    "destination": "NLRTM",
    "promised_at": "2026-03-14T08:00:00Z",
    "sla_tier": "priority",
    "penalty_per_hour": 180.00,
    "customer_tier": "strategic"
  },

  "events": [
    {"t": "2026-03-01T02:00:00Z", "status": "booked",       "location": "CNSHA"},
    {"t": "2026-03-03T14:20:00Z", "status": "in_transit",   "location": "CNSHA",
     "raw_status": "VESSEL DEPARTED"},
    {"t": "2026-03-11T09:00:00Z", "status": "at_hub",       "location": "NLRTM",
     "raw_status": "AWAITING BERTH - CONGESTION"},
    {"t": "2026-03-11T09:00:00Z", "status": "at_hub",       "location": "NLRTM",
     "raw_status": "AWAITING BERTH - CONGESTION",
     "_note": "deliberate duplicate -- idempotency must absorb this"},
    {"t": "2026-03-12T06:00:00Z", "ingest_delay_hours": 9,
     "status": "at_hub", "location": "NLRTM", "raw_status": "STILL AWAITING BERTH",
     "_note": "arrives out of order -- event-time handling must be correct"}
  ],

  "context": {
    "port_status": {"NLRTM": {"congestion": "severe", "avg_wait_hours": 76}},
    "weather":     {"NLRTM": {"condition": "storm", "severity": "high"}}
  },

  "ground_truth": {
    "actually_delivered_at": "2026-03-17T22:00:00Z",
    "actually_late_hours": 86,
    "sla_breached": true,
    "optimal_action": "rebook",
    "optimal_action_cost_usd": 1200,
    "cost_of_no_action_usd": 15480,
    "human_analyst_detected_at": "2026-03-12T11:00:00Z"
  },

  "expectations": {
    "must_detect_exception": true,
    "must_detect_by": "2026-03-11T10:00:00Z",
    "expected_kinds": ["delay", "port_congestion"],
    "min_severity": "high",
    "expected_action_types": ["rebook", "expedite"],
    "must_not_action_types": ["no_action"],
    "max_cost_usd": 1.00,
    "duplicate_events_must_produce_actions": 1
  }
}
```

Three details in that fixture are deliberate and should be in most of your scenarios:

- **A duplicate event.** Idempotency must absorb it silently. `duplicate_events_must_produce_actions: 1` asserts it.
- **An out-of-order event** with `ingest_delay_hours`. Event-time reasoning must handle it. This is the single most common real-world data pathology in this domain.
- **`human_analyst_detected_at`.** The baseline you are actually competing against. Detecting at 09:00 when a human found it at 11:00 is 2 hours of lead time, and lead time is the business value.

### Replay harness

```python
# evals/replay.py

class ReplayHarness:
    """Replays recorded event streams through the real system and scores the result.

    Uses the real workflows, real agents, real policy -- only the clock and the
    external context feeds are substituted. If you mock the agents, you are
    testing your mocks.
    """

    async def run_scenario(self, scenario: Scenario) -> ScenarioResult:
        run_id = f"eval-{scenario.scenario_id}-{uuid4().hex[:8]}"

        # Isolated namespace so eval traffic never pollutes production metrics
        handle = await self._temporal.start_workflow(
            ShipmentLifecycleWorkflow.run,
            scenario.shipment.to_init(),
            id=run_id,
            task_queue="shipment-exceptions",
            namespace="shipment-evals",
        )

        # Deterministic context: the mocked world this scenario describes
        await self._context_store.load(run_id, scenario.context)

        # Feed events in ingest order, honouring the out-of-order delays
        for event in scenario.events_in_ingest_order():
            await handle.signal(ShipmentLifecycleWorkflow.on_event, event)

        outcome = await asyncio.wait_for(handle.result(), timeout=300)
        return self._score(scenario, outcome, await self._collect_trace(run_id))

    def _score(self, scenario, outcome, trace) -> ScenarioResult:
        gt, exp = scenario.ground_truth, scenario.expectations
        detected = outcome.first_exception()

        return ScenarioResult(
            scenario_id=scenario.scenario_id,

            # Detection
            detected=detected is not None,
            detected_on_time=detected and detected.detected_at <= exp.must_detect_by,
            lead_time_hours=hours_between(detected.detected_at, gt.actually_delivered_at)
                            if detected else 0,
            lead_time_vs_human_hours=hours_between(
                detected.detected_at, gt.human_analyst_detected_at) if detected else None,
            kind_correct=detected and detected.kind in exp.expected_kinds,
            severity_ok=detected and severity_ge(detected.severity, exp.min_severity),

            # Decision
            action_correct=outcome.action_type in exp.expected_action_types,
            action_forbidden=outcome.action_type in exp.must_not_action_types,

            # Economics -- the number a business person will ask about
            value_captured_usd=(gt.cost_of_no_action_usd - gt.optimal_action_cost_usd)
                               if outcome.action_type in exp.expected_action_types else 0,

            # Correctness invariants
            duplicate_handling_ok=(outcome.action_count ==
                                   exp.duplicate_events_must_produce_actions),

            # Cost
            cost_usd=trace.total_cost,
            within_budget=trace.total_cost <= exp.max_cost_usd,
            tokens=trace.total_tokens,
            latency_s=trace.wall_clock_seconds,

            # Calibration
            stated_confidence=outcome.confidence,
        )
```

### Suite composition

A useful suite is not 50 happy paths. Aim for roughly:

| Category | Share | Purpose |
|---|---|---|
| True positives (real disruption, action warranted) | 40% | Recall |
| **True negatives (looks bad, resolves itself)** | **30%** | **False alarm rate** |
| Data pathologies (dupes, out-of-order, silence, contradictions) | 15% | Robustness |
| Adversarial (prompt injection in payload text) | 10% | Security |
| Edge cases (missing data, unknown codes, conflicting sources) | 5% | Graceful degradation |

**The 30% true-negative allocation is the one people under-invest in and then regret.** Without it you will optimize purely for recall, ship something that fires constantly, and discover the problem only when operators start ignoring it. Every scenario where the right answer is "do nothing" is worth as much as one where the answer is "rebook".

### Calibration report

Because `confidence` feeds the OPA autonomy decision, it must be *calibrated*, not merely present. When the system says 0.9, it should be right about 90% of the time.

```python
def calibration_report(results: list[ScenarioResult]) -> CalibrationReport:
    buckets = defaultdict(list)
    for r in results:
        buckets[round(r.stated_confidence, 1)].append(r.action_correct)
    return CalibrationReport(
        bins=[(conf, mean(hits), len(hits)) for conf, hits in sorted(buckets.items())],
        ece=expected_calibration_error(buckets),
    )
```

If the 0.9 bucket is right 60% of the time, your autonomy threshold of 0.85 is granting autonomy on unreliable signals. **Either recalibrate the agent or raise the threshold in Rego.** This is the loop that connects evaluation back to policy, and it is the difference between "we have a confidence field" and "our confidence field means something".

### Running evals

```bash
make eval                              # full golden suite
make eval SUITE=adversarial            # security subset
make eval BASELINE=v1.3.0              # regression diff against a tagged run
```

```
$ make eval

Replaying 52 scenarios against shipment-evals ...

DETECTION
  Recall                        0.92   (target > 0.90)   PASS
  Precision                     0.87                     PASS
  False alarm rate              0.13   (target < 0.15)   PASS
  Median lead time             18.4h
  Median lead vs human          6.2h

DECISION
  Action correct                0.81
  Forbidden actions               0                      PASS
  Critic rejection rate         0.11   (band .05-.20)    PASS

ROBUSTNESS
  Duplicate handling           52/52                     PASS
  Out-of-order handling        52/52                     PASS
  Injection resisted             5/5                     PASS

ECONOMICS
  Mean cost / exception        $0.34   (target < $0.50)  PASS
  p95 cost / exception         $0.71
  Value captured             $184,200

CALIBRATION
  ECE                           0.08
  conf 0.9 bucket -> actual     0.86                     OK

REGRESSION vs v1.3.0
  Recall            0.89 -> 0.92   +0.03
  False alarm       0.11 -> 0.13   +0.02   REVIEW
  Cost              $0.29 -> $0.34 +$0.05  REVIEW
```

### Gate changes on this

```yaml
# .github/workflows/eval.yml
- name: Evaluate agent changes
  run: make eval BASELINE=${{ github.event.pull_request.base.sha }}
- name: Enforce quality gates
  run: |
    python -m evals.gate \
      --min-recall 0.90 --max-false-alarm 0.15 \
      --max-cost-per-exception 0.50 --max-regression 0.02
```

**No prompt change, model upgrade, or agent refactor merges without passing this.** That rule is the entire reason the harness exists. Without it, quality drifts invisibly and you find out from a customer.

---

## 4. Cost control

Four layers, from tightest loop to widest:

| Layer | Mechanism | Catches |
|---|---|---|
| Per-agent-call | `Budget.assert_available()` | Runaway single exception |
| Per-agent-per-day | LiteLLM virtual key budget | A broken agent looping across many exceptions |
| Per-tenant-per-month | LiteLLM team budget | Commercial exposure |
| Architectural | Deterministic gate before the reasoning tier | **Everything, and it is the only one that scales** |

The fourth is the one that actually determines your unit economics:

```python
if not assessments.warrants_action(eta):
    return ExceptionOutcome.no_action("below action threshold")
```

Cheap fast-tier reads run on every exception. The expensive reasoning tier is reached only when the cheap layer justifies it. **If reasoning-tier calls scale linearly with event volume, no budget cap will save you** — you have an architecture problem, not a cost problem. Watch `agent_cost_usd_total{model_tier="reasoning"}` against `exceptions_detected_total` and keep the ratio flat.

---

## 5. Grafana dashboards

Three dashboards, three audiences:

**Operations** — open exceptions by severity, approval queue depth and age, autonomous vs approved ratio, detection latency p50/p95, consumer lag, open workflows.

**Agent quality** — per-agent latency and cost, validation retry rate, critic rejection rate over time, confidence distribution, human rejection rate over time. *Human rejection rate is your best live proxy for quality between eval runs* — when it climbs, something regressed and your golden suite did not catch it, which is itself a finding.

**Economics** — cost per exception trend, cost by model tier, value captured (avoided penalties minus action spend), budget exhaustion rate, spend by agent.

---

## Phase 4 checkpoint

- [ ] One exception produces exactly one trace with all agent spans correctly nested
- [ ] Parallel agents appear as siblings, and the trace shows real concurrency
- [ ] Sandbox executions appear inside their requesting agent's span
- [ ] All three Grafana dashboards render with live data
- [ ] `DetectionStalled` fires when the detector is stopped while events keep flowing
- [ ] `make eval` runs 50+ scenarios and prints the full report
- [ ] Scenarios covering duplicates, out-of-order events, and injection all pass
- [ ] A deliberate prompt regression is caught by the CI gate
- [ ] The calibration report shows ECE < 0.15
- [ ] Cost per exception is measured and under target

---

**Next:** [05-DOMAIN-MOCK-INGESTION.md](./05-DOMAIN-MOCK-INGESTION.md) — the domain layer feeding all of this.
