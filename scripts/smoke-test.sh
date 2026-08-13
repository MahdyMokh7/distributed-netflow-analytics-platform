#!/bin/bash
set -euo pipefail

# Path to the hdfs binary inside the namenode container image (bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8)
HDFS_BIN="/opt/hadoop-3.2.1/bin/hdfs"

echo "=========================================="
echo "Nimbo Data Ingester - Smoke Test"
echo "=========================================="

# 1. Start the stack
echo ""
echo "[1] Starting services..."
docker compose --profile full up -d
sleep 30

# 2. Check health
echo ""
echo "[2] Checking health..."
if curl -s http://localhost:8080/actuator/health | grep -q "UP"; then
    echo "✅ Application is healthy"
else
    echo "❌ Application is not healthy"
    exit 1
fi

# 3. Create topic
echo ""
echo "[3] Creating Kafka topic..."
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 \
    --create --topic netflow --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic already exists"

# 4. Produce messages using console producer
echo ""
echo "[4] Producing test messages..."
for i in 1 2 3 4 5; do
    echo "{\"IPV4_SRC_ADDR\":\"192.168.1.10$i\",\"IPV4_DST_ADDR\":\"10.0.0.$i\",\"L4_SRC_PORT\":1234$i,\"L4_DST_PORT\":80,\"PROTOCOL\":6,\"IN_BYTES\":$((i*1024)),\"OUT_BYTES\":$((i*512)),\"FLOW_DURATION_MILLISECONDS\":$((i*1000))}" | \
    docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:9092 --topic netflow
    echo "Produced message $i"
done

# 5. Wait for processing
# rolling.max-age defaults to 2 minutes (see application.yml); we wait for the real
# age-based flush to trigger rather than shortening the production batch policy for this test.
echo ""
echo "[5] Waiting for batch flush (up to rolling.max-age=2m)..."
sleep 150

# 6. Check consumer group
echo ""
echo "[6] Checking consumer group..."
docker compose exec kafka kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --group netflow-java-consumer8-dev \
    --describe 2>/dev/null || echo "Consumer group not yet active"

# 7. Check HDFS
echo ""
echo "[7] Checking HDFS Parquet files..."
HDFS_LS_OUTPUT=$(docker compose exec namenode "$HDFS_BIN" dfs -ls -R /data/test/netflow/ 2>/dev/null || true)
PARQUET_COUNT=$(printf '%s\n' "$HDFS_LS_OUTPUT" | grep -c ".parquet" || true)
PARQUET_COUNT=${PARQUET_COUNT:-0}

if [ "$PARQUET_COUNT" -gt 0 ]; then
    echo "✅ Found $PARQUET_COUNT Parquet files"
    printf '%s\n' "$HDFS_LS_OUTPUT"
else
    echo "❌ No Parquet files found"
    exit 1
fi

# 8. Check logs
echo ""
echo "[8] Checking for successful batch flushes..."
BATCH_COUNT=$(docker compose logs ingester 2>/dev/null | grep -c "Successfully persisted batch" || true)
BATCH_COUNT=${BATCH_COUNT:-0}

if [ "$BATCH_COUNT" -gt 0 ]; then
    echo "✅ Found $BATCH_COUNT successful batch flushes"
else
    echo "⚠️ No batch flushes found"
fi

echo ""
echo "=========================================="
echo "✅ Smoke Test PASSED!"
echo "=========================================="