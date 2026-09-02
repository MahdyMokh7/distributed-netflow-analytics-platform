# Batch Processor Run Prod mode

---

## 1. Build the Spark Assembly JAR
    
```bash
cd ~/Desktop/nimbo/Nimbyte/batch-processor
mvn clean package -DskipTests
```

**Output:** `target/batch-processor-1.0.0-SNAPSHOT-spark.jar`

---

## 2. Transfer to Server (Node3)

```bash
scp target/batch-processor-1.0.0-SNAPSHOT-spark.jar team1@10.10.61.53:/home/team1/
```

---

## 3. SSH to Server and Copy to Shared Location

```bash
ssh team1@10.10.61.53

mv batch-processor-1.0.0-SNAPSHOT-spark.jar batch-processor-spark.jar

# Copy to /tmp (shared directory accessible by kubernetes user)
cp /home/team1/batch-processor-spark.jar /tmp/batch-processor-spark.jar
chmod 644 /tmp/batch-processor-spark.jar

# Verify file exists
ls -la /tmp/batch-processor-spark.jar
```

---

## 4. Switch to Kubernetes User and Copy JAR to Spark Master Pod

```bash
sudo su kubernetes

kubectl cp /tmp/batch-processor-spark.jar team1/spark-spark-master-7b9bb87b78-ntjn4:/tmp/batch-processor-spark.jar

# Verify copy succeeded
kubectl exec -n team1 spark-spark-master-7b9bb87b78-ntjn4 -- ls -la /tmp/batch-processor-spark.jar
```

---

## 5. Run `spark-submit` Inside the Pod

```bash
kubectl exec -it -n team1 spark-spark-master-7b9bb87b78-ntjn4 -- /bin/bash
```

Inside the pod:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/sun.net.util=ALL-UNNAMED" \
  --conf spark.executor.extraJavaOptions="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/sun.net.util=ALL-UNNAMED" \
  --class com.nimbo.batch.BatchProcessorApplication \
  /tmp/batch-processor-spark.jar \
  --spring.profiles.active=prod
```

> **Note:** All Spark configs (`driver-memory`, `executor-memory`, `shuffle-partitions`, `speculation`, etc.) are read from `application-prod.yml`. Only the `--master`, `--deploy-mode`, and `--add-opens` flags are passed.

---

## 6. Verify PostgreSQL Data

**Open a new terminal tab** and SSH to Node3:

```bash
ssh team1@10.10.61.53
sudo su kubernetes
```

**Connect to PostgreSQL master pod:**

```bash
kubectl exec -it -n team1 postgresql-postgresql-0 -- psql -U postgres -d nimbo_db
```

**Inside psql, verify tables and data:**

```sql
-- List all tables
\dt

-- Check count of traffic aggregates
SELECT COUNT(*) FROM traffic_aggregates;

-- View sample data (first 10 rows)
SELECT * FROM traffic_aggregates LIMIT 10;

-- Check batch state (processed files)
SELECT * FROM batch_processor_state ORDER BY processed_at DESC LIMIT 5;

-- Exit
\q
```

---


## Run Batch-Processor dev mode [Complete Flow: Build → Transfer → Run on Node3]

### 1. Build JAR (Local Machine)

```bash
cd ~/Desktop/nimbo/Nimbyte/batch-processor

# Clean and package with tests skipped
mvn clean package -DskipTests
```

JAR location: `target/batch-processor-1.0.0-SNAPSHOT.jar`

---

### 2. Transfer JAR to Node3 (Local Machine)

```bash
scp target/batch-processor.jar team1@10.10.61.53:~/
```

---

### 3. Run on Node3 (Server)

```bash
ssh team1@10.10.61.53

# Run with prod profile
java -jar batch-processor.jar --spring.profiles.active=prod
  
  
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-opens=java.base/sun.net.util=ALL-UNNAMED \
  -jar batch-processor.jar --spring.profiles.active=dev
  
  
docker run -d   --name nimbo-postgres   -e POSTGRES_USER=nimbo_user   -e POSTGRES_PASSWORD=123456   -e POSTGRES_DB=nimbo_db   -p 5432:5432   postgres:16.2
docker exec -it nimbo-postgres psql -U nimbo_user -d nimbo_db

```

## Monitor

- **Spark UI:** `http://10.10.61.71/`
- **Spark History UI:** `http://10.10.61.72/`
- **Logs:** `tail -f batch-processor.log`

spark-ui ip : http://10.10.61.71/
spark-ui history ip : http://10.10.61.72/



## How to Edit on PostgreSQL DB

### Clean path from your local system to `nimbo_db` as `nimbo_batch`

Use **Node1** because your `kubectl` access works there:

```bash
# 1. From your local system
ssh team1@10.10.61.51

# 2. Switch to the Kubernetes user
sudo su kubernetes

# 3. Go to the kubernetes user's home (optional)
cd /home/kubernetes

# 4. Enter the PostgreSQL master pod
kubectl exec -it -n team1 postgresql-postgresql-0 -- \
  psql -U postgres -d nimbo_db
```

