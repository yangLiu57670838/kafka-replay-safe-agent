# ReplaySafe Agent — Research Project PRD

**Suggested GitHub repository:** `kafka-replay-safe-agent`  
**Research title:** Failure Recovery and Idempotent Tool Execution in Kafka-Driven LLM Agents  
**Duration:** Two weeks  
**Status:** Proposed plan

## 1. Purpose

Build a reproducible local research system that measures duplicate external actions and repeated LLM computation when an asynchronous AI agent retries after failures.

The agent consumes support requests from Kafka, classifies each request, retrieves a relevant policy, and creates a simulated ticket through FastAPI. MongoDB stores tickets and durable agent progress.

The project supports practical Kafka learning and produces a research report demonstrating experimental reasoning. It is an empirical systems study, not a claim of a new model architecture or a general solution to distributed transactions.

## 2. Problem

A consumer may create a ticket and crash before committing its Kafka offset. After restarting, it receives the same record and may repeat the action. Producers may also publish multiple records representing the same logical request.

The root cause is a failure boundary between Kafka progress, agent state, and external side effects—not simply an LLM lacking memory. AI adds repeated inference cost and potentially inconsistent decisions across retries.

Checking a database before inserting is insufficient under concurrency. The ticket service must enforce uniqueness atomically.

## 3. Research questions and hypotheses

- **RQ1:** How do recovery strategies affect duplicate tickets, missing work, and correct completion under controlled failures?
- **RQ2:** Does persisting agent decisions reduce repeated LLM calls and token usage?
- **RQ3:** What latency overhead do checkpoints and idempotent tools introduce?
- **Exploratory:** How often do repeated inference attempts change classification, policy selection, or ticket arguments?

Hypotheses:

1. Basic retries produce duplicate tickets in the post-action, pre-commit crash window.
2. Completed-event deduplication leaves a failure window before completion is recorded.
3. Tool-side idempotency prevents duplicate persisted tickets within the action-key scope.
4. Saved decisions reduce repeated inference after a checkpoint, but cannot eliminate repetition before that checkpoint is durable.

## 4. Scope

### In scope

- One local Kafka broker, initially one partition and one consumer.
- Python producer and consumer with manual offset commits.
- Constrained agent workflow: classify, retrieve policy, prepare arguments, call ticket tool.
- FastAPI mock ticket service and local MongoDB.
- Deterministic mock inference and a limited real-LLM evaluation.
- Scripted crashes, lost-response simulation, reproducible experiments, plots, and report.

### Out of scope

- AWS, production high availability, broker failover, Kubernetes, and frontend development.
- Jira or Confluence integration.
- Semantic duplicate detection across different request IDs.
- Training a transformer or implementing unrestricted autonomous planning.
- Global exactly-once guarantees across Kafka, databases, LLM providers, and arbitrary APIs.

Every request in the core benchmark requires exactly one ticket. A single local broker supports consumer-recovery experiments, not cluster-resilience claims.

## 5. Architecture and stack

```text
Python producer → Kafka → Python agent → FastAPI ticket service
                              ↓                    ↓
                        MongoDB ledger       MongoDB tickets
                              ↕
                     Policy retrieval + LLM

Experiment controller → failure injection, restart, evaluation
```

The agent and ticket API run as separate processes. They use separate MongoDB collections and do not share a transaction. The agent accesses tickets through HTTP, never by directly modifying the ticket collection.

| Component | Choice |
|---|---|
| Broker | Official Apache Kafka Docker image, KRaft mode |
| Kafka client | Python `confluent-kafka` |
| Ticket service | FastAPI |
| Persistence | MongoDB with persistent Docker volume |
| Retrieval | Fixed policy corpus with BM25 baseline |
| Inference | Mock adapter plus configurable real-model adapter |
| Infrastructure | Docker Compose |
| Evaluation | Python, JSONL/CSV exports, standard plotting tools |

Pin dependency and container versions during implementation. No GPU or separate vector database is required for the initial scope.

## 6. Identity and guarantee boundaries

The producer creates a stable `event_id` once per logical request and preserves it across retries. Include it in the message body and normally use it as the Kafka key.

Kafka keys control partition routing; they do not deduplicate requests. Two records with different offsets may share one logical request ID.

The runtime derives `action_key = event_id + ':create-ticket'`. The LLM must not generate or modify this key. Do not derive it from variable model-generated ticket text.

Protected mode enforces at most one ticket per action key. Eventual completion is a separate property measured after recovery. Identical content with different request IDs represents distinct work in this study.

Repeated keys with conflicting payloads return HTTP 409 rather than silently overwriting the original ticket. Keep keys and completed records for the entire experiment; expiry is outside scope.

## 7. Data models

### Kafka request

```json
{
  "schema_version": 1,
  "event_id": "request-0042",
  "customer_id": "customer-12",
  "message": "I was charged twice. Please help.",
  "created_at": "2026-11-20T00:00:00Z"
}
```

Ground-truth labels live in evaluator fixtures and are not provided to the agent.

### Agent ledger

- Unique `event_id` within each isolated experiment run.
- Request hash to detect conflicting ID reuse.
- Status: `received`, `decision_saved`, `completed`, or `failed`.
- Saved classification, priority, policy ID, and validated ticket arguments.
- Action key, ticket ID, timestamps, and error information.
- Prompt/model identifiers and inference usage metadata.

Persist the decision and tool arguments together in a single document update. A `received` record does not mean work is complete. Start with sequential processing; concurrent ledger ownership is outside the initial scope.

### Ticket

- Ticket ID, event ID, action key, canonical payload hash.
- Customer ID, category, priority, policy ID, and summary.
- Creation timestamp.

Protected mode uses a unique index on `action_key`. Baselines use separate collections without that index, so database protection cannot accidentally hide their failures.

## 8. FastAPI contract

| Endpoint | Purpose |
|---|---|
| `POST /tickets` | Create a ticket or return an existing one |
| `GET /tickets/{ticket_id}` | Inspect a ticket |
| `GET /health` | Readiness check |

Protected requests include an `Idempotency-Key` header. Ticket creation uses an atomic single-document MongoDB upsert with `$setOnInsert`, backed by the unique index. Ticket fields, action key, and payload hash belong in the same document.

Handle concurrent duplicate-key errors by fetching the existing ticket and comparing its payload hash. Do not reserve the key in one write and create the ticket in a later write.

- New ticket: HTTP 201.
- Identical retry: HTTP 200 and original ticket ID.
- Conflicting payload: HTTP 409.

The service must support a test mode that persists a ticket and then drops or withholds the response. A caller timeout must not be interpreted as proof that creation failed.

## 9. Agent behavior

1. Poll and validate the Kafka record.
2. Derive stable request and action identities.
3. Consult durable progress if the strategy supports it.
4. Classify, retrieve policy, and prepare ticket arguments unless a decision is reusable.
5. Validate structured output and allowed policy IDs.
6. Persist the decision if enabled.
7. Call the ticket API.
8. Persist completion if enabled.
9. Commit the consumed record's offset synchronously.

Set `enable.auto.commit=false` and preserve the consumer group across restarts. Establish starting offsets explicitly per run. `auto.offset.reset=earliest` is not a reset mechanism for groups with valid committed offsets.

Use one in-flight message initially so commits cannot skip unfinished earlier work. Apply bounded retries to transient failures. Expose validation failures and exhausted retries as failed outcomes rather than counting them as successful completion.

Output validation is an agent guardrail. Duplicate prevention is enforced by stable identity, durable progress, and the ticket API's atomic behavior.

## 10. Experimental strategies

| Strategy | Completion deduplication | Saved decisions | Idempotent API |
|---|---:|---:|---:|
| A: basic retry | No | No | No |
| B: completed-event deduplication | Yes | No | No |
| C: durable ledger + idempotent tool | Yes | Yes | Yes |
| D: idempotent tool only | No | No | Yes |

A/B/C are required. D is the preferred ablation: C versus D isolates the value of persisted decisions beyond ticket uniqueness. Repeated inference in D may produce conflicting payloads; record those conflicts.

Use identical requests, model settings, prompts, policies, retry settings, and paired fault schedules. Instrumentation must not become hidden recovery state for baselines.

## 11. Fault scenarios

| Scenario | Injection | What it tests |
|---|---|---|
| Normal | No fault | Quality and overhead |
| Duplicate publication | Same event ID in two records | Logical-request deduplication |
| Pre-checkpoint crash | After inference, before decision save | Inference persistence gap |
| Post-checkpoint crash | After decision save, before API call | Reasoning reuse |
| Post-ticket crash | Ticket created, completion not saved | Duplicate-action window |
| Response loss | Ticket persisted, HTTP response unavailable | Uncertain external outcome |
| Pre-commit crash | Completion saved, Kafka offset not committed | Completed-work reuse |

Inject abrupt process termination at explicit checkpoints. Where a strategy lacks a checkpoint, use the corresponding logical stage. The external controller tracks fired faults so restarts do not create an endless crash loop.

Kafka, MongoDB, and the ticket service remain alive during consumer-crash tests. Their data persists. Restart the same consumer group after a fixed delay. Isolate topics/groups and collections between strategy runs or verify a clean reset.

## 12. Dataset and inference

- Generate 200 labelled support requests across billing, account access, and technical support.
- Provide approximately 10–15 short policy documents with explicit IDs.
- Include varied wording, manually checked labels, and separate development/evaluation subsets.
- Preserve IDs for intentionally repeated submissions.
- Freeze prompts before final evaluation and keep labels out of retrieval context.

Run the full matrix with deterministic mock inference first. Then run a smaller fixed held-out subset with a real LLM. The mock stage validates recovery behavior, not real-model quality.

Record exact model identifiers and settings. Do not substitute cached model responses in a way that conceals repeated inference. Low temperature does not guarantee deterministic decisions.

## 13. Metrics and analysis

Let `t_i` be persisted tickets for logical request `i`, and `N` be unique valid requests.

| Metric | Definition |
|---|---|
| Excess tickets per request | `sum(max(t_i - 1, 0)) / N` |
| Requests with duplicates | `count(t_i > 1) / N` |
| Missing-ticket rate | `count(t_i == 0) / N` at the fixed evaluation deadline |
| Correct completion | Exactly one ticket with correct category and policy |
| LLM calls per request | Attempted inference calls divided by `N` |
| Token usage | Reported input/output tokens, with unknown usage identified |
| Recovery time | Failure injection to durable completion, including fixed restart delay |
| End-to-end latency | Publication to completion; median and p95 |
| Decision drift | Disagreement across repeated decisions for one request |
| Payload conflicts | HTTP 409 responses and affected requests |

Count tickets from MongoDB, not agent logs. Track attempted and completed inference separately because an interrupted call can have uncertain usage. Use mock-service counters or external inference instrumentation where feasible.

Run at least five paired seeded repetitions of the mock matrix. Show run-level distributions and confidence intervals where feasible; retries from one request are not independent samples. Report incomplete requests explicitly rather than silently excluding them from latency results.

Required plots: duplicate/missing outcomes by strategy and scenario, LLM calls per request, and recovery/latency distributions. Generate result tables from scripts.

## 14. Acceptance criteria

- Local infrastructure starts through documented commands and persists data.
- Duplicate publication produces two Kafka offsets with the same logical ID.
- A single published record is replayed after the scripted pre-commit crash.
- Strategy A reproduces duplicate tickets in the targeted failure window.
- Strategy B demonstrates its completion-recording gap.
- Strategy C creates no duplicate tickets in the tested matrix and reports any incomplete work transparently.
- Strategy C reuses saved decisions after checkpoints; pre-checkpoint repetition remains measurable.
- Different IDs create different tickets, even for identical message text.
- Protected API uniqueness, concurrent calls, and conflicting payloads are verified.
- Raw results include configurations, seeds, dependency versions, and model settings.
- Report contains actual findings, limitations, negative results, and reproduction commands.

These are outcomes to verify, not assumptions about results.

## 15. Two-week plan

| Days | Deliverable |
|---|---|
| 1–2 | Local Kafka/MongoDB, producer/consumer, manual commits, replay demo |
| 3–4 | FastAPI modes, unique index, atomic creation and payload-conflict checks |
| 5–6 | Dataset, policies, constrained agent, mock inference, strategies A/B |
| 7–8 | Ledger, strategy C, crash and lost-response recovery |
| 9–10 | Experiment runner, mock matrix, strategy D if feasible |
| 11–12 | Limited real-model runs, analysis, plots, failure inspection |
| 13–14 | Research report, README, reproducibility check, short demo |

If time is limited, preserve A/B/C, duplicate publication, post-ticket crash, and a small real-model subset. Defer extra retrieval features and multiple consumers first.

## 16. Repository layout

```text
kafka-replay-safe-agent/
├── README.md
├── prd.md
├── compose.yaml
├── .env.example
├── pyproject.toml
├── src/
│   ├── producer/
│   ├── agent/
│   ├── ticket_api/
│   └── common/
├── data/
│   ├── policies/
│   └── requests/
├── experiments/
│   ├── configs/
│   ├── runner.py
│   └── evaluate.py
├── tests/
├── results/
│   └── README.md
└── report/
    ├── report.md
    └── figures/
```

Exclude credentials, database volumes, and unnecessary large outputs from Git. Include representative raw artifacts and commands for reproducing the full study. Use synthetic customer data only.

## 17. Research report

Target 5–8 pages plus references and appendix:

1. Abstract and research questions.
2. Background: Kafka replay, idempotency, agent checkpointing.
3. Architecture and guarantee boundaries.
4. Dataset, baselines, fault model, and protocol.
5. Results and ablation analysis.
6. Failure cases, AI decision drift, and cost tradeoffs.
7. Threats to validity and follow-up work.

## 18. Budget and limitations

- No AWS charges for the proposed local setup. Check available RAM and Docker disk usage before implementation.
- Use mock inference for the large matrix. Set a proposed US$10–20 real-model spending cap and size runs from measured token usage; this is not a price quote.
- A single-node MongoDB setup supports the required single-document atomic operation. Cross-document transactions are intentionally unnecessary.
- A ledger cannot alone resolve uncertain external side effects; protected mode depends on ticket-service idempotency.
- Crashes before an inference checkpoint can still repeat reasoning and charges.
- Local results do not establish production-scale throughput or availability.
- Synthetic data and a small model sample limit generalisation.
- This plan requires a focused literature review before claiming research novelty.

## 19. Starting references

- [Kafka quickstart](https://kafka.apache.org/44/getting-started/quickstart/)
- [Kafka design and delivery semantics](https://kafka.apache.org/44/design/design/)
- [MongoDB atomicity](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/)
- [MongoDB unique indexes](https://www.mongodb.com/docs/manual/core/index-unique/)

Verify version-specific documentation during implementation and expand the bibliography with agent-recovery research.
