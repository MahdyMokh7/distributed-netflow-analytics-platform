# 📋 Task Prioritization & Dependency Map

Based on your project status (Week 5 - Kubernetes Migration), here's the prioritized order with dependencies:

---

## 🔥 **Week 3 Tasks (Core Stability)**

### 1. ✅ **Fix Production Run** (ALREADY DONE)
- **Status**: ✅ Complete
- **Dependencies**: None
- **Description**: Production deployment running successfully on Node2 with both jars in place

---

### 2. **Read Files Once (Not 8 Times)** ⭐ HIGH PRIORITY
**Priority**: P0 - Must do before optimization
**Dependencies**: None
**Estimate**: 2 hours

**Description**:
Currently reading Parquet files 8 times (4 cubes × 2 write modes). Optimize to read once and reuse the DataFrame.

**Requirements**:
- Read Parquet files once per batch run
- Cache the DataFrame or reuse for all 4 processors
- Ensure memory doesn't blow up (use `cache()` or `persist()`)
- Unpersist after all processors complete

**Implementation**:
```java
// In BatchJobRunner
Dataset<Row> cachedDf = spark.read().parquet(files).cache();
// Pass cachedDf to all processors instead of file list
// processors.forEach(p -> p.process(cachedDf, batchId));
// cachedDf.unpersist();
```

---

### 3. **HDFS Hourly Partitions** ⭐ HIGH PRIORITY
**Priority**: P0 - Performance critical
**Dependencies**: Task #2 (Read Once)
**Estimate**: 3 hours

**Description**:
Organize Parquet files in HDFS by hour-based folders to speed up file discovery.

**Requirements**:
- Modify `FileNameGenerator` to create path like: `/netflow/parquet/YYYY-MM-DD/HH/filename.parquet`
- Update `FileDiscoveryService` to list only relevant hour folders
- Keep backward compatibility for existing files
- Update parquet-writer to write to partitioned paths

**Implementation**:
```java
// In FileNameGenerator
public String generateFilePath(int partition, Instant timestamp) {
    String date = DateTimeFormatter.ISO_LOCAL_DATE.format(timestamp);
    String hour = String.format("%02d", timestamp.getHour());
    return String.format("/netflow/parquet/%s/%s/netflow-p%02d-%s.parquet", 
                         date, hour, partition, timestamp);
}
```

---

### 4. **Fix REST API Bugs** ⭐ HIGH PRIORITY
**Priority**: P0 - Need to verify
**Dependencies**: None (but needs PostgreSQL data)
**Estimate**: 4 hours

**Description**:
Test and fix REST API endpoints that read from PostgreSQL. You haven't manually run it yet.

**Requirements**:
- Run REST API locally/on K8s
- Test all 4 endpoints with sample data
- Fix any bugs (connection issues, query errors, serialization)
- Add proper error handling
- Add Swagger/OpenAPI documentation

**Endpoints to Test**:
- `GET /api/traffic/{ip}?start=...&end=...`
- `GET /api/connections/{ip}?start=...&end=...`
- `GET /api/top-ips?limit=20&start=...&end=...`
- `GET /api/protocols?start=...&end=...`

---

### 5. **Unit Tests - Batch Processor** ⭐ MEDIUM PRIORITY
**Priority**: P1
**Dependencies**: Task #2, #3 (fixed code)
**Estimate**: 8 hours

**Description**:
Write/update comprehensive unit tests for batch-processor module (you already have the framework).

**Requirements**:
- Test all 4 processors with deterministic data
- Test file discovery (incremental, max files, ordering)
- Test database writes with Testcontainers
- Achieve >85% code coverage
- No mocks - use real PostgreSQL container

---

### 6. **Optimize Batch Size & Parallelism** ⭐ MEDIUM PRIORITY
**Priority**: P1
**Dependencies**: Task #2 (Read Once)
**Estimate**: 3 hours

**Description**:
Optimize the number of Parquet files per batch and the write batch size for executors.

**Requirements**:
- Tune `maxFilesPerBatch` based on average file size
- Tune `batch-size` for PostgreSQL writes (currently 5000)
- Test different values for optimal performance
- Add configuration for tuning

**Config to tune**:
```yaml
batch:
  max-files-per-batch: 10  # Currently 5
postgres:
  batch-size: 10000  # Currently 5000
```

---

### 7. **Unit Tests - REST API** ⭐ MEDIUM PRIORITY
**Priority**: P1
**Dependencies**: Task #4 (REST API fixed)
**Estimate**: 4 hours

**Description**:
Write unit tests for REST API module reading from PostgreSQL.

**Requirements**:
- Test all 4 endpoints
- Use Testcontainers PostgreSQL
- Test edge cases (empty results, invalid dates)
- Test error handling
- Achieve >80% code coverage

---

### 8. **Export Prometheus Metrics** ⭐ LOW PRIORITY
**Priority**: P2
**Dependencies**: None
**Estimate**: 3 hours

**Description**:
Export Prometheus metrics for the batch processor and REST API.

**Requirements**:
- Add `micrometer-registry-prometheus` dependency
- Create custom metrics:
    - `batch_processing_duration_seconds`
    - `batch_files_processed_total`
    - `batch_records_written_total`
    - `batch_errors_total`
- Expose `/actuator/prometheus` endpoint
- Add to Grafana dashboards

**Implementation**:
```java
@Autowired
private MeterRegistry meterRegistry;

// In BatchJobRunner
Timer.Sample sample = Timer.start(meterRegistry);
// ... process ...
sample.stop(Timer.builder("batch.processing.duration")
    .description("Batch processing duration")
    .register(meterRegistry));
```

---

### 9. **Learn Port Forwarding & SSH -D** ⭐ LEARNING
**Priority**: P2 (Learning)
**Dependencies**: None
**Estimate**: 1 hour

**Description**:
Learn and practice port forwarding and SSH dynamic port forwarding for accessing K8s services.

**Key Commands to Learn**:
```bash
# Port forwarding to K8s pod
kubectl port-forward -n team1 pod/postgresql-postgresql-0 5432:5432

# SSH Dynamic Port Forwarding (SOCKS proxy)
ssh -D 1080 team1@10.10.61.52

# Access internal services via proxy
curl --socks5 localhost:1080 http://spark-master.team1:8080
```

---

## 🚀 **Week 5 Tasks (Kubernetes Migration) - Batch Processor Only**

### 10. **Kubernetes Helm Chart - Batch Processor** ⭐ CRITICAL
**Priority**: P0 (Week 5)
**Dependencies**: Task #1 (Production works)
**Estimate**: 6 hours

**Description**:
Create Helm chart for batch-processor module to run on Kubernetes.

**Requirements**:
- Create `helm/batch-processor/` directory structure
- `Chart.yaml` with metadata
- `values.yaml` with all configurable parameters
- `templates/deployment.yaml` for the batch processor
- `templates/configmap.yaml` for application config
- `templates/secret.yaml` for DB credentials
- `templates/service.yaml` (optional for metrics)
- `templates/job.yaml` or `cronjob.yaml` for scheduled runs

**Helm Structure**:
```
helm/batch-processor/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── deployment.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── service.yaml
│   ├── cronjob.yaml (or job.yaml)
│   └── _helpers.tpl
```

**Key Implementation Details**:
- Use `ConfigMap` for `application-prod.yml`
- Use `Secret` for DB credentials
- Use `CronJob` for scheduled batch runs (instead of Spring scheduling)
- Or use `Deployment` with Spring scheduling (current approach)

**ConfigMap Example**:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: batch-processor-config
data:
  application.yml: |
    spring:
      profiles:
        active: prod
    spark:
      master: spark://spark-master.team1:7077
      # ... rest of config
```

---

### 11. **Spark on K8s - Classpath Fix** ⭐ CRITICAL
**Priority**: P0 (Week 5)
**Dependencies**: Task #10
**Estimate**: 4 hours

**Description**:
Ensure the PostgreSQL driver and required JARs are available on Spark executors in K8s.

**Options**:
1. **Use HDFS for JARs** (Recommended)
   ```yaml
   spark:
     jars-path: hdfs://10.10.61.53:8020/jars/batch-processor-spark.jar
   ```

2. **Use Init Container**
    - Copy JAR to shared volume before main container starts

3. **Use PVC with pre-loaded JAR**
    - PersistentVolumeClaim with JAR available

**Requirements**:
- JAR must be available on all executor pods
- Test with `Class.forName("org.postgresql.Driver")`
- Ensure no more `ClassNotFoundException`

---

### 12. **CI/CD Pipeline - Jenkins** ⭐ CRITICAL
**Priority**: P0 (Week 5)
**Dependencies**: Task #10, #11
**Estimate**: 4 hours

**Description**:
Create Jenkins pipeline for building and deploying batch-processor to K8s.

**Pipeline Stages**:
1. **Checkout** - Pull code from Git
2. **Build** - `mvn clean package -DskipTests`
3. **Test** - Run unit/integration tests
4. **Docker Build** - Build Docker image
5. **Push** - Push image to registry
6. **Helm Deploy** - Deploy to K8s with Helm
7. **Verify** - Health check the deployment

**Jenkinsfile**:
```groovy
pipeline {
    agent any
    environment {
        DOCKER_REGISTRY = 'registry.nimbo.com'
        K8S_NAMESPACE = 'team1'
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn clean package -DskipTests'
            }
        }
        stage('Docker Build') {
            steps {
                sh 'docker build -t ${DOCKER_REGISTRY}/batch-processor:${BUILD_NUMBER} .'
            }
        }
        stage('Helm Deploy') {
            steps {
                sh 'helm upgrade --install batch-processor ./helm/batch-processor --set image.tag=${BUILD_NUMBER}'
            }
        }
    }
}
```

---

### 13. **Self-Healing - Liveness/Readiness Probes** ⭐ CRITICAL
**Priority**: P0 (Week 5 Challenge)
**Dependencies**: Task #10
**Estimate**: 2 hours

**Description**:
Configure proper health checks for automatic recovery.

**Requirements**:
- **Liveness Probe**: `/actuator/health/liveness`
- **Readiness Probe**: `/actuator/health/readiness`
- **Startup Probe**: For slow-starting containers

**Implementation**:
```yaml
# In deployment.yaml
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  initialDelaySeconds: 60
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 5
```

---

### 14. **Zero-Downtime - Rolling Update Strategy** ⭐ CRITICAL
**Priority**: P0 (Week 5 Challenge)
**Dependencies**: Task #10
**Estimate**: 1 hour

**Description**:
Configure rolling updates for zero-downtime deployments.

**Requirements**:
```yaml
# In deployment.yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1
    maxUnavailable: 0
```

**Test**: Deploy new version and verify no requests are dropped.

---

### 15. **Resource Limits & Scaling** ⭐ MEDIUM
**Priority**: P1 (Week 5)
**Dependencies**: Task #10
**Estimate**: 2 hours

**Description**:
Configure resource limits for pod stability.

**Requirements**:
```yaml
# In deployment.yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "1"
  limits:
    memory: "4Gi"
    cpu: "2"
```

**Horizontal Pod Autoscaler** (Optional):
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: batch-processor-hpa
spec:
  minReplicas: 1
  maxReplicas: 3
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

---

## 📊 Priority Matrix

| Task | Priority | Dependencies | Week |
|------|----------|--------------|------|
| Fix Production Run | ✅ Done | None | N/A |
| Read Once | P0 | None | 3 |
| HDFS Hourly Partitions | P0 | Read Once | 3 |
| Fix REST API | P0 | None | 3 |
| Unit Tests - Processor | P1 | Read Once, HDFS | 3 |
| Optimize Batch Size | P1 | Read Once | 3 |
| Unit Tests - REST API | P1 | Fix REST API | 3 |
| Export Prometheus Metrics | P2 | None | 3 |
| Learn Port Forwarding | P2 | None | 3 |
| **Helm Chart - Processor** | **P0** | **Prod works** | **5** |
| **Spark Classpath Fix** | **P0** | **Helm Chart** | **5** |
| **CI/CD Pipeline** | **P0** | **Helm Chart** | **5** |
| **Self-Healing Probes** | **P0** | **Helm Chart** | **5** |
| **Zero-Downtime** | **P0** | **Helm Chart** | **5** |
| **Resource Limits** | **P1** | **Helm Chart** | **5** |

---

## 🗓️ Suggested 2-Week Sprint Plan

### Week 1 (Current Week 3 Tasks)
| Day | Tasks |
|-----|-------|
| **Day 1** | Read Once optimization |
| **Day 2** | HDFS Hourly Partitions |
| **Day 3** | Fix REST API |
| **Day 4** | Unit Tests - Processor |
| **Day 5** | Optimize Batch Size |

### Week 2 (Week 5 Preparation)
| Day | Tasks |
|-----|-------|
| **Day 1** | Helm Chart - Batch Processor |
| **Day 2** | Spark Classpath Fix on K8s |
| **Day 3** | CI/CD Pipeline |
| **Day 4** | Self-Healing Probes |
| **Day 5** | Zero-Downtime + Resource Limits |