Here's the updated README with the batch-processor run instructions replaced:

---

# Nimbo NetFlow Platform

**A production-grade, end-to-end data processing pipeline for network telemetry analytics**

![Platform: Kubernetes](https://img.shields.io/badge/Platform-Kubernetes-blue)
![Java: 17](https://img.shields.io/badge/Java-17-blue)
![Spark: 4.1.1](https://img.shields.io/badge/Spark-4.1.1-orange)
![PostgreSQL: 16.2](https://img.shields.io/badge/PostgreSQL-16%2B-blue)
![StarRocks: 4.0.12](https://img.shields.io/badge/StarRocks-4.0.12-purple)
![REST API: Spring Boot](https://img.shields.io/badge/REST%20API-Spring%20Boot-brightgreen)
![Spring Boot: 3.4.3](https://img.shields.io/badge/Spring%20Boot-3.4.3-brightgreen)
![Kafka: 3.7.2](https://img.shields.io/badge/Kafka-3.7.2-red)
![HDFS: Hadoop 3.3.6](https://img.shields.io/badge/HDFS-Hadoop%203.3.6-yellow)
![Ansible: Automation](https://img.shields.io/badge/Ansible-Automation-orange)
![Prometheus: Monitoring](https://img.shields.io/badge/Prometheus-Monitoring-red)
![Grafana: Dashboards](https://img.shields.io/badge/Grafana-Dashboards-blue)

---

## 📖 Project Overview

Nimbo is a Lambda Architecture-based platform for ingesting, processing, and querying NetFlow data at scale. The system ingests Avro-encoded network flow records from Kafka, persists them as columnar Parquet files, and provides both **batch** and **real-time** query capabilities through independent analytical paths.

### The Problem We Solve

Network operations and security teams need two fundamentally different types of queries:

- **Operational queries:** "What is this IP doing right now?" — answered in seconds
- **Analytical queries:** "How does this behavior compare to last month?" — requires pre-aggregated data

Nimbo's dual-path architecture serves both use cases without compromising on data integrity or performance.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              NIMBO DATA PIPELINE                                   │
│                                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │
│  │  Kafka   │───▶│   Ingester   │───▶│    HDFS     │───▶│   Spark     │         │
│  │ (Avro)   │    │ (Consumer)   │    │  (Parquet)  │    │  Processor  │         │
│  └──────────┘    └──────────────┘    └──────────────┘    └──────┬───────┘         │
│         │               │                       │                 │                 │
│         │               │                       │                 ▼                 │
│         │               │                  PROCESSING_TIME    ┌──────────────┐      │
│         │               │                  column added      │ PostgreSQL   │      │
│         │               │                  at ingestion      │ 4 Aggregate  │      │
│         │               │                                    │ Tables       │      │
│         │               ▼                                    └──────────────┘      │
│         │         ┌──────────────┐                                  │              │
│         └────────▶│  StarRocks   │──────────┐                      │              │
│                   │  Writer      │          │                      │              │
│                   └──────────────┘          │                      │              │
│                                              ▼                      ▼              │
│                                     ┌──────────────────────────────────────┐      │
│                                     │           REST API Layer            │      │
│                                     │  ┌──────────┐  ┌──────────┐        │      │
│                                     │  │ Batch    │  │Realtime │        │      │
│                                     │  │ Queries  │  │ Queries │        │      │
│                                     │  └──────────┘  └──────────┘        │      │
│                                     └──────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

- **Zero Data Loss:** Manual offset commits only after successful persistence
- **Horizontal Scalability:** Multi-consumer architecture with parallel writers
- **Exactly-Once Semantics:** Atomic file operations + UPSERT with `batch_id` tracking
- **Fault Tolerance:** Graceful shutdown with flush on termination, retry with backoff
- **Auditability:** Every row tagged with `batch_id` for complete traceability

---

## 📦 Modules

| Module | Description | Status |
|--------|-------------|--------|
| **batch-ingester** | Kafka → HDFS Parquet consumer with zero data loss | ✅ Complete |
| **batch-processor** | Spark aggregations → PostgreSQL (Cube Maker) | ✅ Complete |
| **realtime** | Kafka → StarRocks low-latency writer | ✅ Complete |
| **rest-api** | Unified REST API for both batch and real-time queries | ✅ Complete |

---

## 🎯 Core Aggregations

The platform supports four key network analytics queries:

1. **Traffic Aggregation** — Bandwidth consumption per source IP per time window
2. **Connection Aggregation** — 5-tuple connection details per time window
3. **Protocol Usage** — L7 protocol usage statistics per time window
4. **Top IP Bandwidth** — IPs with highest bandwidth consumption (bidirectional)

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|------------|
| **Data Ingestion** | Kafka, Avro, Spring Boot |
| **Storage (Batch)** | HDFS, Parquet |
| **Storage (Realtime)** | StarRocks |
| **Batch Processing** | Apache Spark 4.1.1, Scala 2.13 |
| **Analytical DB** | PostgreSQL 15+ with range partitioning |
| **API Layer** | Spring Boot, JDBI, JPA, Resilience4j |
| **Containerization** | Docker, Jib |
| **Orchestration** | Kubernetes |
| **CI/CD** | Jenkins, Gerrit, SonarQube |

---

## 👥 Team

| Role | Name | Responsibility |
|------|------|----------------|
| **Batch Path Lead** | **Mehdy Mokhtari** | Batch Ingester (Consumer), Batch Processor (Spark + PostgreSQL), REST API (Batch Queries) |
| **Realtime Path Lead** | Sobhan Arshadi | Realtime Module (StarRocks Writer), REST API (Realtime Queries), DLQ Handling |
| **Infrastructure Lead** | Amin Jangi | Batch Ingester (Storage Layer), CI/CD Pipeline, DevOps, Containerization |

---

## 📁 Project Structure

```
nimbo/
├── batch-ingester/          # Kafka → HDFS consumer
│   ├── consumer/            # Multi-instance Kafka consumer
│   ├── deserializer/        # Avro deserialization
│   ├── storage/             # HDFS + Parquet writing
│   └── service/             # Validation, DLQ
│
├── batch-processor/         # Spark → PostgreSQL aggregations
│   ├── processor/           # 4 aggregation processors
│   ├── writer/              # JDBI-based PostgreSQL writer
│   ├── discovery/           # Incremental file discovery
│   └── model/               # Domain models
│
├── realtime/                # Kafka → StarRocks writer
│   ├── consumer/            # StarRocks batch processor
│   ├── mapper/              # Avro → StarRocks mapping
│   └── client/              # StarRocks Stream Load client
│
├── rest-api/                # Unified REST API
│   ├── controller/          # Versioned REST endpoints
│   ├── service/             # Query services with caching
│   ├── repository/          # JPA + StarRocks repositories
│   └── dto/                 # Request/Response DTOs
│
└── docs/                    # Technical documentation
    ├── architecture/        # Design decisions
    ├── dev-guide/           # Development guides
    ├── technical-report/    # Weekly reports
    └── servers/             # Infrastructure documentation
```

---

## 🚀 Quick Start

### Prerequisites

- Java 17+
- Docker & Docker Compose
- Access to Kafka cluster (`10.10.61.51:9092`, `10.10.61.53:9092`, `10.10.61.54:9092`)
- Access to HDFS (`hdfs://10.10.61.53:8020`)
- PostgreSQL (`10.10.61.70:5432`)

### Build

```bash
# Build all modules
mvn clean package -DskipTests

# Build specific module
cd batch-processor && mvn clean package -DskipTests
```

### Run Batch Processor

**Option 1: Run with Spring Boot (Development)**

```bash
cd batch-processor
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

**Option 2: Run with Spark Submit (Production)**

```bash
# Transfer JAR to Node3
scp target/batch-processor.jar team1@10.10.61.53:~/

# SSH into Node3 and run
ssh team1@10.10.61.53

# Run with prod profile
java -jar batch-processor.jar --spring.profiles.active=prod

# Or with custom schedule
java -jar batch-processor.jar \
  --spring.profiles.active=prod \
  --batch.schedule="0 */1 * * * ?"
```

**Option 3: Run in Docker with Spark**

```bash
docker run -it --rm \
  --network host \
  -v /home/team1/batch-processor-1.0.0-SNAPSHOT.jar:/app/batch-processor.jar \
  zoraq/spark:4.1.1 \
  spark-submit \
  --master spark://10.10.61.71:7077 \
  --deploy-mode client \
  --class com.nimbo.batch.BatchProcessorApplication \
  /app/batch-processor.jar \
  --spring.profiles.active=prod
```

### Connect to PostgreSQL

**Option 1: Via Kubernetes (Production)**

```bash
# SSH to Node1
ssh team1@10.10.61.51

# Switch to Kubernetes user
sudo su kubernetes

# Enter PostgreSQL master pod
kubectl exec -it -n team1 postgresql-postgresql-0 -- \
  psql -U postgres -d nimbo_db
```

**Option 2: Via Docker (Local Development)**

```bash
# Run PostgreSQL container
docker run -d \
  --name nimbo-postgres \
  -e POSTGRES_USER=nimbo_user \
  -e POSTGRES_PASSWORD=123456 \
  -e POSTGRES_DB=nimbo_db \
  -p 5432:5432 \
  postgres:16.2

# Connect to PostgreSQL
docker exec -it nimbo-postgres psql -U nimbo_user -d nimbo_db
```

### Run REST API

```bash
cd rest-api
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

### Deploy with Docker

```bash
# Build and push images
mvn compile jib:build

# Run with Docker Compose (local dev)
docker-compose --profile full up -d
```

---

## 📊 Database Schema

The analytical store consists of five tables:

| Table | Purpose | Partition Key |
|-------|---------|---------------|
| `traffic_aggregates` | Source IP bandwidth per window | `window_start` |
| `connection_aggregates` | 5-tuple connection details | `window_start` |
| `protocol_usage_aggregates` | L7 protocol usage statistics | `window_start` |
| `ip_bandwidth_aggregates` | IP bandwidth (bidirectional) | `window_start` |
| `batch_processor_state` | Tracks processed Parquet files | — |

All aggregate tables use `batch_id` (UUID) in their primary keys for idempotency and auditability.

---

## 🔄 CI/CD Pipeline

- **Build:** Maven with Jib for containerization
- **Quality Gates:** SonarQube (minimum 80% coverage)
- **Code Review:** Gerrit (mandatory for all changes)
- **Deployment:** Jenkins → Kubernetes

---

## 📈 Monitoring & Observability

| Component | Endpoint |
|-----------|----------|
| **Spark UI** | `http://10.10.61.71/` |
| **Spark History UI** | `http://10.10.61.72/` |
| **PostgreSQL** | `10.10.61.70:5432` |
| **REST API Actuator** | `/api/actuator/health` |
| **StarRocks** | `10.10.61.51:8040` (stream load) |

---

## 📚 Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Two Avro schemas** (wire + storage) | Prevent `EOFException` on Kafka deserialization |
| **JDBI over manual JDBC** | Type-safe, 90% less code, maintainable |
| **PROCESSING_TIME in ingester** | Consistent across reprocessing, correct for windowing |
| **Fixed-width filename format** | Simple timestamp extraction, no parsing ambiguity |
| **Sort-before-cap in file discovery** | Prevents file starvation with backlog |
| **Decimal(38,0) for aggregations** | No overflow, keeps ANSI enabled |
| **batch_id in PRIMARY KEY** | Idempotency + auditability + exactly-once semantics |

---

## 📝 License

This project is proprietary and confidential to the Nimbo team.

---

## 🙏 Acknowledgments

Built with guidance from senior engineers at the company bootcamp. Special thanks to the TPMs for their challenging questions during weekly reviews.

---

*Last Updated: September 2026*
