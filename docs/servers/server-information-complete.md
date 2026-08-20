# Complete Cluster Inventory Report

## 📊 System Overview Comparison

| Category | Node1 (10.10.61.51) | Node2 (10.10.61.52) | Node3 (10.10.61.53) | Node4 (10.10.61.54) |
|----------|---------------------|---------------------|---------------------|---------------------|
| **Hostname** | team1-node1 | team1-node2 | team1-node3 | team1-node4 |
| **OS** | Ubuntu 22.04.3 LTS | Ubuntu 22.04.3 LTS | Ubuntu 22.04.3 LTS | Ubuntu 22.04.3 LTS |
| **Kernel** | 5.15.0-161-generic | 5.15.0-161-generic | 5.15.0-161-generic | 5.15.0-161-generic |
| **Architecture** | x86_64 | x86_64 | x86_64 | x86_64 |
| **Virtualization** | QEMU | QEMU | QEMU | QEMU |
| **CPU** | Xeon Silver 4314 @ 2.40GHz | Xeon Silver 4314 @ 2.40GHz | Xeon Silver 4314 @ 2.40GHz | Xeon Silver 4314 @ 2.40GHz |
| **CPU Cores** | 4 | 4 | 4 | 4 |
| **RAM Total** | 16 GB | 16 GB | 16 GB | 16 GB |
| **RAM Used** | 10.3 GB | 9.9 GB | 10.0 GB | 8.6 GB |
| **RAM Available** | 5.3 GB | 5.8 GB | 5.7 GB | 7.1 GB |
| **Swap** | 0 B (disabled) | 0 B (disabled) | 0 B (disabled) | 0 B (disabled) |
| **Uptime** | 16 days | 16 days | 16 days | 16 days |
| **Load Average** | 1.38, 0.84, 0.71 | 0.59, 0.71, 0.77 | 2.76, 1.15, 0.70 | 0.40, 0.41, 0.37 |

---

## 💾 Storage Comparison

| Mount Point | Node1 (10.10.61.51) | Node2 (10.10.61.52) | Node3 (10.10.61.53) | Node4 (10.10.61.54) |
|-------------|---------------------|---------------------|---------------------|---------------------|
| **Root (/) Usage** | 71% (21 GB/32 GB) | 71% (21 GB/32 GB) | 71% (21 GB/32 GB) | **62%** (19 GB/32 GB) |
| **Root Available** | 8.8 GB | 8.8 GB | 8.8 GB | 12 GB |
| **/mnt/disk1 Usage** | 10% (34 GB/393 GB) | 10% (34 GB/393 GB) | 10% (34 GB/393 GB) | 9% (33 GB/393 GB) |
| **/mnt/disk1 Available** | 339 GB | 339 GB | 339 GB | 340 GB |
| **Disk Write Speed** | 290 MB/s | Not tested | 269 MB/s | **318 MB/s** |

---

## 🔧 Running Services by Node

### Core Infrastructure Services Matrix

| Service | Node1 (10.10.61.51) | Node2 (10.10.61.52) | Node3 (10.10.61.53) | Node4 (10.10.61.54) |
|---------|:---:|:---:|:---:|:---:|
| **Kafka Broker** | ✅ (9092) | ✅ (9092) | ✅ (9092) | ✅ (9092) |
| **Kafka Controller** | ✅ (9096) | ✅ (9096) | ✅ (9096) | ✅ (9096) |
| **ZooKeeper** | ❌ | ✅ (2181, 3888) | ✅ (2181, 3888) | ✅ (2181, 2888, 3888) |
| **HDFS NameNode** | ✅ (9870, 8020) | ✅ (9870, 8020) | ✅ (9870, 8020) | ❌ |
| **HDFS DataNode** | ✅ (9864, 9866, 9867) | ✅ (9864, 9866, 9867) | ✅ (9864, 9866, 9867) | ✅ (9864, 9866, 9867) |
| **HDFS JournalNode** | ✅ (8480, 8485) | ✅ (8480, 8485) | ✅ (8480, 8485) | ❌ |
| **HDFS ZKFC** | ✅ (8019) | ✅ (8019) | ✅ (8019) | ❌ |
| **HDFS HTTPFS** | ✅ (14000) | ✅ (14000) | ✅ (14000) | ❌ |
| **StarRocks BE** | ✅ (8060, 8040, 9050, 9060, 9070) | ✅ (8060, 8040, 9050, 9060, 9070) | ✅ (8060, 8040, 9050, 9060, 9070) | ✅ (8060, 8040, 9050, 9060, 9070) |
| **StarRocks FE** | ✅ (9010, 9020, 9030, 8030) | ✅ (9010, 9020, 9030, 8030) | ✅ (9010, 9020, 9030, 8030) | ✅ (9010, 9020, 9030, 8030) |
| **Docker** | ✅ | ✅ | ✅ | ✅ |
| **containerd** | ✅ | ✅ | ✅ | ✅ |
| **SSH** | ✅ (22) | ✅ (22) | ✅ (22) | ✅ (22) |
| **Keepalived** | ✅ | ❌ | ✅ | ✅ |

### Kubernetes Control Plane Services

| Service | Node1 (10.10.61.51) | Node2 (10.10.61.52) | Node3 (10.10.61.53) | Node4 (10.10.61.54) |
|---------|:---:|:---:|:---:|:---:|
| **Kube API Server** | ✅ (6443) | ✅ (6443) | ✅ (6443) | ❌ |
| **etcd** | ✅ (2379, 2380) | ✅ (2379, 2380) | ✅ (2379, 2380) | ❌ |
| **Kube Scheduler** | ✅ (10259) | ✅ (10259) | ✅ (10259) | ❌ |
| **Kube Controller Mgr** | ✅ (10257) | ✅ (10257) | ✅ (10257) | ❌ |
| **Kubelet** | ✅ (10248, 10250) | ✅ (10248, 10250) | ✅ (10248, 10250) | ✅ (10248, 10250) |
| **Kube Proxy** | ✅ (10249, 10256) | ✅ (10249, 10256) | ✅ (10249, 10256) | ✅ (10249, 10256) |

### Kubernetes Pods Distribution

| Pod | Node1 (10.10.61.51) | Node2 (10.10.61.52) | Node3 (10.10.61.53) | Node4 (10.10.61.54) |
|-----|:---:|:---:|:---:|:---:|
| **calico-node** | ✅ (7qcb2) | ✅ (mslp2) | ✅ (664ht) | ✅ (gtq97) |
| **calico-typha** | ✅ | ❌ | ❌ | ❌ |
| **coredns** | ✅ (48b7r) | ❌ | ✅ (krwdw) | ❌ |
| **etcd** | ✅ (node1) | ✅ (node2) | ✅ (node3) | ❌ |
| **kube-apiserver** | ✅ (node1) | ✅ (node2) | ✅ (node3) | ❌ |
| **kube-controller-manager** | ✅ (node1) | ✅ (node2) | ✅ (node3) | ❌ |
| **kube-proxy** | ✅ (4kqdn) | ✅ (8kc4h) | ✅ (g5h9w) | ✅ (mg8b6) |
| **kube-scheduler** | ✅ (node1) | ✅ (node2) | ✅ (node3) | ❌ |
| **metallb-controller** | ❌ | ❌ | ✅ | ❌ |
| **metallb-speaker** | ❌ | ❌ | ❌ | ✅ |
| **openebs-localpv** | ❌ | ❌ | ❌ | ✅ |
| **ostorlab** | ❌ | ❌ | ❌ | ✅ |
| **postgresql-postgresql** | ✅ (replica) | ✅ (replica) | ❌ | ✅ (**master**) |
| **postgresql-operator** | ❌ | ❌ | ❌ | ✅ |
| **spark-history-server** | ❌ | ❌ | ❌ | ✅ |
| **spark-master** | ❌ | ❌ | ❌ | ✅ |
| **spark-worker** | ✅ (xhlrk) | ✅ (9ltnp) | ✅ (mlpb6) | ✅ (wr6lz) |
| **zookeeper** | ❌ | ✅ (zookeeper-1) | ✅ (zookeeper-0) | ✅ (zookeeper-2) |

---

## 📦 PostgreSQL Cluster Information

| Role | Node | IP | Pod Name | Status |
|------|------|-----|----------|--------|
| **Master** | Node4 | 10.244.25.150 | postgresql-postgresql-0 | ✅ Running |
| **Replica** | Node2 | 10.244.227.68 | postgresql-postgresql-1 | ✅ Running |
| **Replica** | Node1 | 10.244.233.66 | postgresql-postgresql-2 | ✅ Running |

**PostgreSQL Service Details:**
| Service | Type | Cluster IP | External IP | Port |
|---------|------|------------|-------------|------|
| postgresql | ClusterIP | 10.100.167.184 | - | 5432 |
| postgresql-master | LoadBalancer | 10.105.29.212 | 10.10.61.70 | 5432 |
| postgresql-replica | ClusterIP | 10.108.146.128 | - | 5432 |
| patroni | ClusterIP | 10.100.225.173 | - | 8000 |

---

## 🗄️ HDFS Cluster Status

### Overall HDFS Status

| Metric | Value |
|--------|-------|
| **Total Capacity** | 1.38 TB |
| **Used** | 31.36 GB |
| **Available** | 1.27 TB |
| **Used %** | 2.35% |
| **Replication Factor** | 3 |
| **Block Size** | 128MB |
| **HDFS URI** | hdfs://10.10.61.53:8020 |

### HDFS DataNode Distribution

| Node | Capacity | Used | Available | Used % | Blocks |
|------|----------|------|-----------|--------|--------|
| **Node1 (10.10.61.51)** | 353.38 GB | 7.84 GB | 325.53 GB | 2.22% | 97 |
| **Node2 (10.10.61.52)** | 353.38 GB | 6.93 GB | 326.44 GB | 1.96% | 93 |
| **Node3 (10.10.61.53)** | 353.38 GB | 8.50 GB | 324.87 GB | 2.41% | 106 |
| **Node4 (10.10.61.54)** | 353.38 GB | 8.09 GB | 325.28 GB | 2.29% | 103 |

### HDFS Netflow Data

| Location | Details |
|----------|---------|
| **Root Directory** | `/netflow/` |
| **Parquet Directory** | `/netflow/parquet/` |
| **Total Files** | 13 Parquet files (+1 temp) |
| **Total Data** | ~3.3 GB |
| **Average File Size** | ~254 MB |
| **Replication** | 3x (total on disk ~10 GB) |
| **Date Range** | Aug 14, 2026 (13:46 - 14:08) |

---

## 🗄️ Persistent Volumes (PV) & Claims (PVC)

### PostgreSQL PVs

| PV Name | Capacity | Access Mode | Status | Bound to PVC | Node |
|---------|----------|-------------|--------|--------------|------|
| pvc-128ed659-3970-4048-836e-4bf4cfe9cbe1 | 5Gi | RWO | Bound | data-postgresql-postgresql-0 | Node4 |
| pvc-c89b1a1a-8bb4-45ad-af68-ceb44d1e6049 | 5Gi | RWO | Bound | data-postgresql-postgresql-1 | Node2 |
| pvc-98093f1c-005d-4c73-bb18-0aae4ff81380 | 5Gi | RWO | Bound | data-postgresql-postgresql-2 | Node1 |

### ZooKeeper PVs

| PV Name | Capacity | Access Mode | Status | Bound to PVC | Node |
|---------|----------|-------------|--------|--------------|------|
| pvc-3d7139e6-a607-45f5-93c4-198c6dd22283 | 5G | RWO | Bound | zookeeper-zookeeper-0 | Node3 |
| pvc-b0bfca8a-0bd9-401d-95a6-53b8377e0e0c | 5G | RWO | Bound | zookeeper-zookeeper-1 | Node2 |
| pvc-0f65ba6d-ba27-44b2-87c1-8d4946a31ee4 | 5G | RWO | Bound | zookeeper-zookeeper-2 | Node4 |

---

## 🌐 Spark Cluster Information

| Role | Node | IP | Port | Status |
|------|------|-----|------|--------|
| **Spark Master** | Node4 | 10.244.25.156 | 7077, 8080 | ✅ Running |
| **Spark History Server** | Node4 | 10.244.25.155 | 18080 | ✅ Running |
| **Spark Worker** | Node1 | 10.10.61.51 | 8081 | ✅ Running |
| **Spark Worker** | Node2 | 10.10.61.52 | 8081 | ✅ Running |
| **Spark Worker** | Node3 | 10.10.61.53 | 8081 | ✅ Running |
| **Spark Worker** | Node4 | 10.10.61.54 | 8081 | ✅ Running |

**Spark Service Details:**
| Service | Type | Cluster IP | External IP | Ports |
|---------|------|------------|-------------|-------|
| spark-master | LoadBalancer | 10.99.218.230 | 10.10.61.71 | 80, 7077, 6066 |
| spark-spark-history-server | ClusterIP | 10.109.248.112 | - | 80 |

---

## 🔌 Port Summary by Node

### Node1 (10.10.61.51) - team1-node1
| Service | Ports |
|---------|-------|
| **HDFS NameNode** | 9870, 8020 |
| **HDFS DataNode** | 9864, 9866, 9867 |
| **HDFS JournalNode** | 8480, 8485 |
| **HDFS ZKFC** | 8019 |
| **HDFS HTTPFS** | 14000 |
| **Kafka** | 9092 |
| **Kafka Controller** | 9096 |
| **StarRocks BE** | 8060, 8040, 9050, 9060, 9070 |
| **StarRocks FE** | 9010, 9020, 9030, 8030 |
| **Kubernetes API** | 6443 |
| **etcd** | 2379, 2380 |
| **kubelet** | 10248, 10250 |
| **kube-proxy** | 10249, 10256 |
| **kube-scheduler** | 10259 |
| **kube-controller** | 10257 |
| **Spark Worker** | 8081 |
| **JMX Exporter** | 7074 |
| **Calico** | 9099 |
| **SSH** | 22 |

### Node2 (10.10.61.52) - team1-node2
| Service | Ports |
|---------|-------|
| **Same as Node1** | All HDFS, StarRocks, Kafka, Kubernetes services |
| **Additional - ZooKeeper** | 2181, 3888 |
| **ZooKeeper Admin** | 8080 |
| **SSH** | 22 |

### Node3 (10.10.61.53) - team1-node3
| Service | Ports |
|---------|-------|
| **Same as Node1** | All HDFS, StarRocks, Kafka, Kubernetes services |
| **Additional - ZooKeeper** | 2181, 3888 |
| **ZooKeeper Admin** | 8080 |
| **SSH** | 22 |

### Node4 (10.10.61.54) - team1-node4
| Service | Ports |
|---------|-------|
| **HDFS DataNode** | 9864, 9866, 9867 |
| **Kafka** | 9092 |
| **Kafka Controller** | 9096 |
| **StarRocks BE** | 8060, 8040, 9050, 9060, 9070 |
| **StarRocks FE** | 9010, 9020, 9030, 8030 |
| **kubelet** | 10248, 10250 |
| **kube-proxy** | 10249, 10256 |
| **ZooKeeper** | 2181, 2888, 3888 |
| **Spark Master** | 7077, 8080 |
| **Spark Worker** | 8081 |
| **Spark History Server** | 18080 |
| **Metallb Speaker** | 7946, 8000 |
| **JMX Exporter** | 7074 |
| **Calico** | 9099 |
| **SSH** | 22 |

---

## 📦 Installed Software Versions

| Software | Version | Location |
|----------|---------|----------|
| **Hadoop** | 3.3.6 | `/opt/namenode-3.3.6`, `/opt/datanode-3.3.6`, `/opt/journalnode-3.3.6` |
| **Kafka** | 2.13-3.7.2 | `/opt/kafka_2.13-3.7.2` |
| **StarRocks** | 4.0.12 | `/opt/StarRocks-4.0.12-ubuntu-amd64` |
| **Spark** | Latest (K8s) | Containerized |
| **ZooKeeper** | v3.5.21 | `/opt/etcd-v3.5.21` |
| **JDK 8** | 1.8.0_361 | `/opt/jdk1.8.0_361` |
| **JDK 17** | 17.0.6 | `/opt/jdk-17.0.6` |
| **PostgreSQL** | Latest (K8s) | Containerized (Patroni) |
| **Docker** | Latest | `/usr/bin/docker` |
| **Kubernetes** | Latest | kubelet, kubectl |
| **etcd** | v3.5.21 | `/opt/etcd-v3.5.21` |

---

## 🎯 Kafka Configuration

| Setting | Value |
|---------|-------|
| **Topic** | `netflow` |
| **Partitions** | 3 |
| **Replication Factor** | 2 |
| **Retention** | 30 days |
| **Consumer Group** | `netflow-java-consumer8` |
| **Bootstrap Servers** | `10.10.61.51:9092,10.10.61.53:9092,10.10.61.54:9092` |
| **Kafka Install Path** | `/opt/kafka/bin/` |
| **HDFS URI** | `hdfs://10.10.61.52:9000` |
| **HDFS Base Path** | `/data/netflow/` |

### Partition Distribution

| Partition | Leader Node | Replicas |
|-----------|-------------|----------|
| **Partition 0** | Node4 (10.10.61.54) | Node4, Node1 |
| **Partition 1** | Node1 (10.10.61.51) | Node1, Node3 |
| **Partition 2** | Node2 (10.10.61.52) | Node2, Node4 |

---

## 👤 System Users

| User | Home Directory | Services |
|------|----------------|----------|
| **root** | /root | System services |
| **hadoop** | /home/hadoop | HDFS services |
| **kafka** | /home/kafka | Kafka services |
| **starrocks** | /home/starrocks | StarRocks services |
| **kubernetes** | /home/kubernetes | Kubernetes management |
| **team1** | /home/team1 | Main user |
| **vagrant** | /home/vagrant | VM management |

---

## 📊 Key Findings Summary

### ✅ Strengths
- All services are running without errors
- HDFS healthy with 1.27 TB free space
- High availability for Kafka (3 brokers) and PostgreSQL (1 master + 2 replicas)
- ZooKeeper cluster with 3 nodes
- All nodes have same OS and kernel version
- Good network connectivity between all nodes
- PostgreSQL port (5432) available on all nodes

### ⚠️ Observations
- Root disk at **62-71%** usage - monitor closely
- Swap disabled on all nodes
- Node4 is the most specialized (PostgreSQL Master + Spark Master)
- Node1 is the only node without ZooKeeper
- HDFS commands not found in PATH on some nodes
- StarRocks BE memory usage is high (16-18% of RAM)


## Custom

- kubectl get svc -n team1 | grep postgresql
- postgresql                     ClusterIP      10.100.167.184   <none>        5432/TCP                                     16d
- postgresql-config              ClusterIP      None             <none>        <none>                                       16d
- postgresql-master              LoadBalancer   10.105.29.212    10.10.61.70   5432:32422/TCP                               16d
- postgresql-replica             ClusterIP      10.108.146.128   <none>        5432/TCP                                     16d

| Category                         | Key Information                                                                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Hadoop Installation**          | `/opt/namenode-3.3.6`                                                                                                                                        |
| **Hadoop Version**               | **3.3.6**                                                                                                                                                    |
| **Hadoop Binary**                | `/opt/namenode-3.3.6/bin/hadoop`                                                                                                                             |
| **Hadoop User**                  | `hadoop`                                                                                                                                                     |
| **Hadoop Node**                  | `team1-node3`                                                                                                                                                |
| **Kubernetes Namespace**         | `team1`                                                                                                                                                      |
| **Spark Master Pod**             | `spark-spark-master-7b9bb87b78-ntjn4`                                                                                                                        |
| **Spark Master Status**          | `Running` (`1/1` containers ready)                                                                                                                           |
| **Spark Master Pod Age**         | `17d`                                                                                                                                                        |
| **Spark Master Pod IP**          | `10.244.25.156`                                                                                                                                              |
| **Spark Master Kubernetes Node** | **`team1-node4`**                                                                                                                                            |
| **Spark Version**                | **4.1.1**                                                                                                                                                    |
| **Scala Version**                | 2.13.17                                                                                                                                                      |
| **Java Version**                 | 17.0.6                                                                                                                                                       |
| **Kubernetes Namespace Check**   | Successful when using the `kubernetes` user                                                                                                                  |
| **Important Issue**              | `hadoop` user received `NotFound: the server could not find the requested resource` from `kubectl`; the `kubernetes` user successfully accessed the cluster. |


### 2.1 Server Inventory

| Server          | Role  | HDFS                | Kafka    | Spark    | PostgreSQL | Kubernetes      |
| --------------- | ----- | ------------------- | -------- | -------- | ---------- | --------------- |
| **10.10.61.51** | Node1 | ✅ DataNode          | ✅ Broker | ✅ Worker | ❌          | ✅ Control Plane |
| **10.10.61.52** | Node2 | ✅ NameNode          | ❌        | ✅ Worker | ✅ Replica  | ✅ Control Plane |
| **10.10.61.53** | Node3 | ✅ NameNode (Active) | ✅ Broker | ✅ Worker | ❌          | ✅ Control Plane |
| **10.10.61.54** | Node4 | ✅ DataNode          | ✅ Broker | ✅ Master | ✅ Master   | ❌               |
