## Server Discovery Summary

| Server | Kafka | HDFS | Zookeeper | Partition Leader | Role |
|--------|-------|------|-----------|------------------|------|
| **10.10.61.51** | ✅ Yes | ❌ No | ❌ No | Partition 1 | Kafka Broker |
| **10.10.61.52** | ❌ No | ✅ Yes | ❌ No | Partition 2 | HDFS Namenode + Kafka Leader |
| **10.10.61.53** | ✅ Yes | ❌ No | ✅ Yes | None (Replica) | Kafka Broker + Zookeeper |
| **10.10.61.54** | ✅ Yes | ❌ No | ❌ No | Partition 0 | Kafka Broker |

### Kafka Configurations
| Setting | Value |
|---------|-------|
| **Topic** | `netflow` |
| **Partitions** | 3 |
| **Replication Factor** | 2 |
| **Retention** | Changed from 7 days to **30 days** |
| **Consumer Group** | `netflow-java-consumer8` |
| **Kafka Install Path** | `/opt/kafka/bin/` |

### Jenkinsfile Configuration
```groovy
KAFKA_BOOTSTRAP_SERVERS = '10.10.61.51:9092,10.10.61.53:9092,10.10.61.54:9092'
HDFS_URI = 'hdfs://10.10.61.52:9000'
KAFKA_TOPIC = 'netflow'
KAFKA_CONSUMER_GROUP = 'netflow-java-consumer8'
HDFS_BASE_PATH = '/data/netflow/'
```