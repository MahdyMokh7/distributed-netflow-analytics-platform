#!/usr/bin/env bash
set -e

hdfs namenode -format -force

hdfs namenode > /tmp/namenode.log 2>&1 &
NAMENODE_PID=$!

echo "Waiting for NameNode..."
until curl -sf http://localhost:9870/ > /dev/null; do
    sleep 1
done

echo "NameNode is ready."

hdfs datanode > /tmp/datanode.log 2>&1 &
DATANODE_PID=$!

echo "Waiting for DataNode registration..."

until hdfs dfsadmin -report 2>/dev/null \
    | grep -q "Live datanodes (1):"; do
    sleep 1
done

echo "HDFS_READY"

wait $NAMENODE_PID $DATANODE_PID