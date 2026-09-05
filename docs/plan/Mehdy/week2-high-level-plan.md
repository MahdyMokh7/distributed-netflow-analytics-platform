# Week 2 High-Level Plan: Batch Path Implementation

## Your Responsibilities
Spark Cube Maker: Read Parquet from HDFS → Process with Spark → Write to PostgreSQL

---

## Section 0: Infrastructure Setup (Day 1)

### 0.1. PostgreSQL Installation & Configuration
- Install PostgreSQL 15+ on server **10.10.61.52**
- Configure PostgreSQL for production:
  - Set `max_connections = 100` (or higher based on expected load)
  - Configure `shared_buffers = 25%` of available RAM
  - Enable WAL archiving for durability
  - Set up proper authentication (md5/scram-sha-256)
  - Create dedicated database user `nimbo_user` with limited privileges
- Enable remote access from Spark worker nodes (configure `pg_hba.conf`)
- Set up systemd service for automatic restart
- Create database: `nimbo_analytics`

### 0.2. Spark Standalone Cluster Setup
- **Master Node Selection**: 10.10.61.52 (same as PostgreSQL - already has low load)
- **Worker Nodes**: 10.10.61.51, 10.10.61.53, 10.10.61.54 (utilize existing Kafka brokers)
- **Spark Version**: 3.5.4 (latest stable compatible with Hadoop 3.4.1)
- Download and extract Spark to `/opt/spark`
- Configure environment variables: `SPARK_HOME`, `PATH`, `JAVA_HOME`
- Create Spark configuration files:
  - `spark-env.sh`: Set `JAVA_HOME`, `SPARK_MASTER_HOST`, worker resources
  - `spark-defaults.conf`: Configure executor memory, cores, serialization
- Start master and worker services with systemd
- Verify cluster with Spark UI (port 8080)

### 0.3. HDFS Verification
- Confirm Phase 1 Parquet files exist at `/netflow/parquet/`
- Verify partition structure and file naming convention
- Test reading a sample Parquet file with Spark shell
- Document HDFS URI: `hdfs://10.10.61.52:9000`

---

## Section 1: Database Schema Design (Day 1-2)

### 1.1. Schema Requirements Analysis
Based on Phase 1 analytics requirements, design tables for:

**Table: `ip_traffic_hourly`**
- Primary aggregation for bandwidth consumption
- Columns: `ip`, `direction` (inbound/outbound), `hour_timestamp`, `total_bytes`, `total_packets`, `record_count`
- Partition by `hour_timestamp` (monthly partitions)
- Index on `(ip, hour_timestamp, direction)`

**Table: `ip_connections`**
- For "list of connections for an IP"
- Columns: `src_ip`, `dst_ip`, `src_port`, `dst_port`, `protocol`, `timestamp`, `bytes`, `packets`
- Consider: This table will grow very large. Options:
  - **Option A**: Store raw connection records (fast query, huge storage)
  - **Option B**: Aggregate by 5-minute windows (balanced)
  - **Option C**: Daily aggregates only (less storage, slower queries)
  - **Recommendation**: Option B (5-minute aggregates) for production balance

**Table: `protocol_stats_hourly`**
- For "most used protocols"
- Columns: `protocol`, `hour_timestamp`, `total_bytes`, `total_packets`, `connection_count`
- Index on `(hour_timestamp, protocol)`

**Table: `top_ip_daily`**
- For "highest traffic IP"
- Columns: `ip`, `date`, `inbound_bytes`, `outbound_bytes`, `total_bytes`, `rank` (optional)
- Materialized daily for fast query performance

### 1.2. DDL Scripts
- Create migration scripts with rollback capability
- Include proper constraints, defaults, comments
- Use `CREATE TABLE IF NOT EXISTS` for idempotency
- Include pg_stat_statements for query performance monitoring

---

## Section 2: Spark Job Design (Day 2-3)

### 2.1. Project Structure

```
nimbo-batch-processor/
├── pom.xml (with Spark dependencies)
├── src/main/java/com/nimbo/batch/
│   ├── config/
│   │   ├── SparkConfig.java
│   │   └── PostgresConfig.java
│   ├── processor/
│   │   ├── TrafficAggregationProcessor.java
│   │   ├── ConnectionProcessor.java
│   │   └── ProtocolProcessor.java
│   ├── repository/
│   │   ├── TrafficRepository.java
│   │   ├── ConnectionRepository.java
│   │   └── ProtocolRepository.java
│   ├── model/
│   │   ├── NetflowAggregate.java
│   │   ├── ConnectionAggregate.java
│   │   └── ProtocolAggregate.java
│   └── job/
│       └── BatchJobRunner.java
└── src/main/resources/
    ├── application.yml
    └── log4j2.xml
```

### 2.2. Configuration Design

**application.yml** structure:
```yaml
spark:
  master: spark://10.10.61.52:7077
  app-name: nimbo-batch-processor
  batch-size: 1000000
  partition-count: 8

hdfs:
  uri: hdfs://10.10.61.52:9000
  parquet-path: /netflow/parquet/

postgres:
  host: 10.10.61.52
  port: 5432
  database: nimbo_analytics
  username: ${DB_USERNAME}
  password: ${DB_PASSWORD}
  pool-size: 20

batch:
  schedule: "0 0 * * * ?"  # Hourly - configurable
  window-hours: 1           # Process last 1 hour by default
  retention-days: 30        # Keep 30 days of detailed data
  enabled: true
```

### 2.3. Processor Implementation Strategy

**TrafficAggregationProcessor:**
- Read Parquet files from HDFS for the time window
- Use Spark SQL to group by: `ip`, direction, `window(timestamp, '1 hour')`
- Write to `ip_traffic_hourly` table using DataFrame writer
- Use `mode(SaveMode.Append)`

**ConnectionProcessor:**
- Read Parquet files
- Aggregate by 5-minute windows to manage data volume
- Write to `ip_connections` table
- Consider using `repartition()` before writing for parallelism

**ProtocolProcessor:**
- Group by protocol and hour window
- Calculate bytes, packets, connection counts
- Write to `protocol_stats_hourly`

**TopIPDaily:**
- Read from `ip_traffic_hourly` for the day
- Calculate top N IPs
- Write to `top_ip_daily`

### 2.4. Error Handling & Recovery
- Implement checkpointing for Spark jobs
- Use try-catch blocks with proper logging
- Implement dead-letter handling for failed records
- Add retry mechanism for PostgreSQL writes with exponential backoff
- Configure JDBC batch size for optimal performance

---

## Section 3: Testing Strategy (Day 3-4)

### 3.1. Unit Tests
- Mock Spark sessions for testing transformations
- Test individual aggregation logic with sample data
- Use Spark's `LocalSparkSession` for unit tests
- Test edge cases: null values, empty datasets, malformed data

### 3.2. Integration Tests
- Spin up Testcontainers for PostgreSQL
- Use embedded HDFS (mini-cluster) or mock filesystem
- Test full end-to-end flow with sample Parquet files
- Validate database writes with assertions

### 3.3. Performance Tests
- Test with 1M records to measure throughput
- Profile Spark job with different batch sizes
- Monitor PostgreSQL write performance
- Document optimal configuration settings

---

## Section 4: Production Readiness (Day 4-5)

### 4.1. Logging & Monitoring
- Set up structured logging with MDC for correlation
- Log Spark job metrics: read count, write count, duration
- Implement health checks for Spark connectivity and HDFS access
- Expose metrics for Prometheus/Graphite if needed

### 4.2. Deployment Strategy

**Option A: Spring Boot Application**
- Package as executable JAR
- Runs as long-lived process with scheduled tasks
- Built-in health checks and graceful shutdown

**Option B: Spark Submit Job**
- Package as assembly JAR
- Submit via `spark-submit` with cron/scheduler
- Simpler, but less monitoring control

**Recommendation:** Option A for better operational control

### 4.3. Docker Compose Integration
- Create Dockerfile for batch processor
- Add to docker-compose.yml alongside existing services
- Configure environment variables for deployment

### 4.4. Configuration Management
- Support multiple profiles: `dev`, `test`, `prod`
- Use environment variables for secrets
- Provide example `.env` file
- Configure log levels per environment

### 4.5. Documentation
- README with setup instructions
- Spark job execution guide
- Database schema documentation
- Troubleshooting guide for common issues

---

## Section 5: Handover & Validation (Day 5)

### 5.1. Validation Checklist
- [ ] PostgreSQL installed and configured
- [ ] Spark cluster running with all workers
- [ ] HDFS path accessible from Spark
- [ ] Sample Parquet file successfully read
- [ ] All aggregation jobs run successfully
- [ ] Data written to PostgreSQL correctly
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Logging working as expected
- [ ] Configuration externalized
- [ ] Docker image builds successfully
- [ ] Documentation complete

### 5.2. Knowledge Transfer
- Document Spark UI URL and monitoring
- Provide PostgreSQL connection details
- Explain how to trigger batch job manually
- Document how to adjust batch window config
- Share common troubleshooting steps

---

## Technology Stack for Your Module

| Component | Version | Reason |
|-----------|---------|--------|
| Spark | 3.5.4 | Latest stable with Hadoop 3.4.1 support |
| PostgreSQL | 15.x | Stable, mature, good analytical support |
| Spark SQL | 3.5.4 | Native DataFrame API for aggregations |
| HDFS | 3.4.1 | Existing infrastructure |
| Parquet | 1.15.0 | Existing format |

## Dependencies (Maven)

```xml
<dependencies>
    <!-- Spark Core & SQL -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.13</artifactId>
        <version>3.5.4</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.13</artifactId>
        <version>3.5.4</version>
        <scope>provided</scope>
    </dependency>
    
    <!-- PostgreSQL -->
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>42.7.3</version>
    </dependency>
    
    <!-- Hadoop (provided in Spark env) -->
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>3.4.1</version>
        <scope>provided</scope>
    </dependency>
    
    <!-- Configuration & Logging -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
        <version>3.4.3</version>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <version>1.18.36</version>
    </dependency>
    
    <!-- Testing -->
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers</artifactId>
        <version>1.21.4</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>postgresql</artifactId>
        <version>1.21.4</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

---

## Critical Success Factors

1. **Zero Data Loss**: Spark job must process all Parquet files correctly, with retry on failure
2. **Idempotency**: Running the same job twice should not create duplicate records
3. **Monitoring**: Logs must show progress and errors clearly
4. **Performance**: Hourly job must complete within 15-20 minutes
5. **Scalability**: Spark cluster should handle increased data volume
