# Kafka Commerce Platform — Application Stack & Requirements

## 1. Project Overview

Build a lightweight, distributed, event-driven e-commerce simulation using **Apache Kafka**, with a comprehensive observability stack based on **VictoriaMetrics and Grafana**.

The platform simulates realistic business processes, including order creation, payments, inventory management, shipping, and customer notifications.

A dedicated **Control Panel** allows users to trigger traffic spikes, service failures, network disruptions, and other unexpected events to observe their impact on the distributed system.

### Primary Goals

- Demonstrate event-driven microservice architecture using Kafka.
- Run the entire platform locally using Docker Compose.
- Minimize memory and CPU requirements.
- Support scaling to multiple Kafka brokers and service replicas.
- Provide meaningful business and infrastructure metrics.
- Simulate realistic failure scenarios and traffic patterns.
- Visualize system behavior through Grafana dashboards.
- Support repeatable chaos experiments and performance tests.

---

## 2. Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Message broker | Apache Kafka (KRaft) | Distributed event streaming |
| Microservices | Go | Lightweight business services |
| Frontend | React + Vite | Control Panel UI |
| Backend API | Go | Scenario orchestration |
| Metrics collection | vmagent | Scraping and forwarding |
| Metrics storage | VictoriaMetrics | Time-series database |
| Visualization | Grafana | Dashboards and monitoring |
| Kafka metrics | JMX Exporter | Broker/JVM metrics |
| Consumer metrics | Kafka Exporter | Consumer lag and topic metrics |
| Container orchestration | Docker Compose | Local development and deployment |
| Application metrics | Prometheus Go client | Custom service metrics |

All core components should use free, open-source editions.

## 3. Distributed Application Architecture

### Business Microservices

**1. Order Generator**
- Generates synthetic customer orders.
- Supports configurable transactions per second.
- Simulates realistic order values, product distributions, and customer behavior.
- Publishes events to `orders.created`.

**2. Payment Service**
- Consumes incoming orders.
- Simulates payment authorization.
- Supports configurable processing latency and failure probability.
- Publishes `payments.completed` or `payments.failed`.

**3. Inventory Service**
- Consumes orders and reserves products.
- Tracks simulated stock levels.
- Supports inventory shortages and reservation failures.
- Publishes `inventory.reserved` or `inventory.rejected`.

**4. Shipping Service**
- Waits for successful payment and inventory reservation.
- Simulates fulfillment and shipment creation.
- Publishes `orders.shipped`.
- Supports shipping delays and temporary warehouse outages.

**5. Notification Service**
- Processes order, payment, and shipping updates.
- Simulates emails, SMS messages, or push notifications.
- Can intentionally operate slowly to generate consumer lag.

**6. Analytics Service**
- Consumes business events from multiple topics.
- Calculates sales volume, revenue, error rates, and order throughput.
- Exposes custom Prometheus metrics.

### Kafka Topics

| Topic | Initial partitions |
|---|---:|
| `orders.created` | 6 |
| `payments.completed` | 6 |
| `payments.failed` | 3 |
| `inventory.reserved` | 6 |
| `inventory.rejected` | 3 |
| `orders.shipped` | 6 |
| `orders.dlq` | 3 |

Use `order_id` as the partition key for order-related events. Each microservice has its own consumer group, with multiple instances sharing work when scaled.

Consumers should handle duplicate messages safely. The shipping workflow must correlate payment and inventory outcomes before creating shipments.

---

## 4. Chaos & Load Control Panel

Create a simple web application called **Kafka Chaos Control**.

The Control Panel is the central interface for managing load tests, injecting failures, and observing system behavior.

### Core Features

- Start, stop, and reset the simulation.
- Adjust incoming transactions per second.
- Enable or disable individual microservices.
- Configure artificial processing delays.
- Change service failure probabilities.
- Trigger predefined business scenarios.
- Scale selected consumer services.
- View active experiments and their duration.
- Restore the system to its normal configuration.
- Display links to relevant Grafana dashboards.

### Scenario Library

| Scenario | Triggered behavior | Expected effect |
|---|---|---|
| **Black Friday** | Increase orders from 50/sec to 1,000/sec | Throughput spikes, consumer lag grows |
| **Payment Provider Down** | Payment service returns errors for 90 seconds | Failed payments, stalled orders |
| **Warehouse Overload** | Inventory processing becomes 10× slower | Inventory consumer lag |
| **Flash Sale** | Sudden demand for one popular product | Inventory contention, stockouts |
| **Kafka Broker Failure** | Stop one Kafka broker | Leader elections, temporary disruption |
| **Network Chaos** | Inject latency or packet loss for a selected service | Retries, timeouts, backlog growth |
| **Notification Meltdown** | Slow notification processing to 1 event/sec | Rapidly increasing consumer lag |
| **Poison Message** | Inject malformed or invalid events | Processing errors, DLQ activity |
| **Duplicate Delivery** | Re-publish selected events | Idempotency testing |
| **Consumer Crash Loop** | Repeatedly restart a selected consumer | Rebalances, lag fluctuations |
| **Traffic Tsunami** | Gradually increase traffic beyond processing capacity | Sustained backlog growth |
| **Silent Failure** | Stop one service from making progress without crashing | Growing lag despite healthy process status |
| **Database Slowdown** | Delay simulated application storage operations | Increased end-to-end latency |
| **Regional Outage** | Disable a selected group of services | Partial system outage and recovery |
| **Recovery Storm** | Resume multiple paused consumers simultaneously | Catch-up traffic and resource spikes |

### Scenario Configuration

Each scenario should have configurable parameters:

- Target service or Kafka component
- Intensity
- Duration
- Start delay
- Automatic recovery
- Optional randomization
- Expected observable symptoms

Example:

```yaml
scenario: black-friday
duration: 180s
parameters:
  orders_per_second: 1000
  payment_latency_ms: 150
  inventory_latency_ms: 100
  popular_product_probability: 0.7
recovery:
  enabled: true
  restore_previous_settings: true
```

### Safety Requirements

- Chaos experiments must be disabled by default.
- Destructive actions must be limited to the local demo environment.
- Broker failures and network disruptions must require explicit confirmation.
- Experiments should have configurable timeouts and automatic recovery.
- The Control Panel must not expose unrestricted Docker access.
- The orchestrator must use an allowlist of permitted actions.
- Scenario execution must generate audit events and metrics.

---

## 5. Observability Architecture

### Metrics Pipeline

```text
Kafka Brokers ── JMX Exporter ──┐
                                │
Kafka Exporter ─────────────────┤
                                │
Go Microservices ── /metrics ───┼── vmagent
                                │       │
Control Panel ──── /metrics ────┘       │
                                        ▼
                                VictoriaMetrics
                                        │
                                        ▼
                                     Grafana
```

### Monitoring Categories

**Kafka Infrastructure**
- Broker availability
- Partition leaders and replication
- Under-replicated partitions
- Messages per second
- Network throughput
- Request latency
- JVM memory and garbage collection

**Consumer Health**
- Consumer-group lag
- Lag growth rate
- Processing throughput
- Consumer rebalances
- Consumer failures
- Message processing latency

**Business Metrics**
- Orders per second
- Successful and failed payments
- Revenue per minute
- Inventory reservation success rate
- Orders awaiting shipment
- End-to-end order completion latency
- Dead-letter queue volume

**Chaos Experiment Metrics**
- Active scenario
- Scenario start and end time
- Configured failure intensity
- Affected services
- Time to recovery
- Observed service degradation

Grafana dashboards should correlate technical incidents with business impact.

---

## 6. Docker Compose Deployment

The initial environment should contain:

**Infrastructure**
- 3 Kafka brokers in KRaft mode
- 1 VictoriaMetrics instance
- 1 vmagent
- 1 Grafana instance
- Kafka Exporter
- JMX Exporter integration

**Application**
- Order Generator
- Payment Service
- Inventory Service
- Shipping Service
- Notification Service
- Analytics Service

**Control**
- React Control Panel
- Go Scenario Orchestrator

### Deployment Requirements

- Use Docker Compose as the primary orchestration mechanism.
- Support configurable Kafka broker count and service replicas.
- Use named volumes for persistent Kafka and metrics data.
- Provide preconfigured Kafka topics and Grafana data sources.
- Support automatic startup through a single command.
- Keep business services independently deployable.
- Prefer minimal container images and resource-efficient implementations.
- Provide health checks and structured logs.
- Allow additional load generator instances without changing the architecture.

The initial deployment should target a development machine with approximately 8–16 GB RAM, with a reduced single-broker profile for lower-memory environments. Actual requirements must be measured during implementation.

---

## 7. Control Panel Design

The UI should prioritize usability and clarity over visual complexity.

### Main Dashboard

Display:
- Current simulation status
- Transactions per second
- Active Kafka brokers
- Active services
- Consumer lag overview
- Current failure scenarios
- Recent incidents
- Quick links to Grafana

### Scenario Launcher

Present scenarios as cards with:
- Scenario name and description
- Severity indicator
- Configurable parameters
- Start and stop controls
- Automatic recovery settings

### Experiment Timeline

Display a chronological history of:
- Scenario activation
- Configuration changes
- Service disruptions
- Recovery events
- Relevant Kafka and application alerts

### Live Feedback

The panel should distinguish between **requested actions** and **observed effects**.

For example, triggering a broker shutdown does not automatically mean Kafka is unavailable. The application should show whether the broker actually stopped and direct users to the resulting metrics.

---

## 8. Scalability Requirements

The system should scale gradually without requiring a major architectural redesign.

**Stage 1 — Local Development**
- Docker Compose
- Single Kafka broker
- One instance per application service
- Single-node VictoriaMetrics
- Modest synthetic traffic

**Stage 2 — Distributed Simulation**
- Three Kafka brokers
- Replicated Kafka topics
- Multiple consumer replicas
- Increased event throughput
- Failure injection and recovery experiments

**Stage 3 — Larger Deployments**
- Multiple hosts or Kubernetes
- Independently scaled microservices
- Distributed metrics collection
- VictoriaMetrics cluster deployment when necessary
- More sophisticated chaos orchestration

Scaling must account for Kafka partition counts, consumer-group behavior, and resource limits.

---

## 9. Suggested Development Milestones

### Phase 1 — Minimum Viable Platform
- Docker Compose infrastructure
- Single Kafka broker
- Order Generator
- Payment and Inventory Services
- VictoriaMetrics, vmagent, Grafana
- Basic business and Kafka dashboards

### Phase 2 — Full Business Workflow
- Shipping, Notification, and Analytics Services
- Event correlation and idempotent processing
- Dead-letter queue handling
- Complete business metrics

### Phase 3 — Control Panel
- React interface
- Scenario Orchestrator API
- Configurable traffic profiles
- Artificial latency and failure injection
- Scenario history and recovery controls

### Phase 4 — Distributed Chaos Testing
- Three-broker Kafka cluster
- Broker failure simulations
- Consumer crash loops
- Network fault injection
- Recovery experiments
- Enhanced Grafana dashboards

### Phase 5 — Advanced Capabilities
- Scheduled scenario sequences
- Randomized chaos experiments
- Performance benchmarking
- Experiment result comparisons
- Automated resilience checks
- Optional Kubernetes deployment

---

## 10. Definition of Done

The initial project is successful when:

1. The platform starts reproducibly using Docker Compose.
2. Synthetic orders flow through the full business workflow.
3. Kafka broker and application metrics appear in Grafana.
4. The Control Panel can trigger and stop multiple scenarios.
5. A Black Friday scenario demonstrably increases throughput.
6. A slow consumer produces observable consumer lag.
7. A service outage produces measurable business errors.
8. A Kafka broker failure can be demonstrated in the three-broker setup.
9. Scenarios automatically restore their temporary configuration.
10. The entire system can be reset to a reproducible baseline.

## 11. Long-Term Vision

**Kafka Commerce Platform** should become a compact, reproducible playground for event-driven architecture, distributed systems, monitoring, and failure engineering.

The core experience is:

**Generate realistic business activity → Trigger unexpected events → Observe Kafka and microservice behavior → Analyze failure and recovery in Grafana.**

The project should remain lightweight enough for local development while demonstrating patterns relevant to real production systems.
