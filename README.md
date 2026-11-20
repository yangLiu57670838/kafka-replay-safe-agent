# ReplaySafe Agent

Failure recovery and idempotent tool execution in a Kafka-driven LLM agent.

## What we watch for in Kafka

The agent can create a ticket and still die before it commits the Kafka offset. After restart, Kafka delivers the same record again. The model may run again, and the ticket tool may run again.

Three writes each succeed or fail on their own: the Kafka offset, the agent ledger, and the ticket. A message key only picks the partition, so two records can carry one logical request and still arrive as two deliveries. The producer can also publish that same request twice.

```mermaid
sequenceDiagram
    participant Kafka
    participant Agent
    participant Ledger
    participant Ticket as Ticket API

    Kafka->>Agent: Deliver one record
    Agent->>Agent: Infer classification and policy
    Note over Agent: Crash here repeats inference
    Agent->>Ledger: Save the decision
    Note over Agent,Ticket: Crash here can reuse the saved decision
    Agent->>Ticket: Create the ticket
    Note over Ticket,Ledger: Crash or a lost response leaves the side effect uncertain
    Agent->>Ledger: Save completion
    Note over Kafka,Agent: Crash here replays work that already finished
    Agent->>Kafka: Commit the offset
```

## How an AI agent can go wrong when working with Kafka

Kafka still owes the agent that record, because the agent never confirmed it was finished.

A record stays on the partition after the agent reads it. Kafka advances the consumer group's position only when that group commits an offset. The commit means everything before this offset is done. Until the commit lands, a restart receives the same record again.

The agent commits only after its own work. For one request the order is:

1. Kafka delivers `request-0042` at offset 10.
2. The agent calls the model and creates the ticket.
3. The agent commits offset 11.
4. Only then does Kafka treat offset 10 as finished.

If the process dies at step 2, step 3 never happens. Kafka's stored position is still offset 10. The new process joins the same consumer group, so Kafka delivers `request-0042` again. The ticket from the first attempt is already stored, and the second attempt can create another one and call the model again.

That second delivery is the duplicate. Kafka treats an unconfirmed record as unfinished work, so it sends the record again.

## What this study tests

The study measures how recovery strategies change duplicate tickets, missing work, repeated model calls, and latency when that record comes back.

The same requests and the same fault schedule run under four strategies:

| Strategy | What is turned on |
|---|---|
| A Basic retry | Retry only |
| B Completed-event dedup | A durable completion record |
| C Ledger and idempotent tool | Saved decisions, plus one ticket per action key |
| D Idempotent tool only | One ticket per action key, with no saved decision |

The runs test four claims:

1. Basic retry creates a second ticket when the crash falls after the ticket exists and before the offset commit.
2. A completion record still leaves a gap before that record is durable.
3. An idempotent ticket API keeps a single stored ticket for one action key.
4. A saved decision cuts repeat inference only after that checkpoint is durable. A crash before it still pays for inference again.

The report records what the runs show, including incomplete work and cases that cut against these claims.
