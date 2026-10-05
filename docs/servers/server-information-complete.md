# Complete Cluster Inventory Report

> Live, read-only snapshot collected on **2026-09-06 at approximately 08:22–08:27 IRST** from `10.10.61.51`–`10.10.61.54`.
> This document records observed state, not desired state. No credentials are stored here.

## Executive summary

The four-node cluster is reachable and its base infrastructure is generally operational. All Kubernetes nodes are `Ready`; Kafka has four reachable brokers/controllers and the `netflow` topic is fully in sync; HDFS has four healthy DataNodes and no missing or under-replicated blocks. The main exceptions are three Kubernetes workloads: the PostgreSQL instance on Node1 is affected by an image pull failure, while the Spark workers on Node1 and Node3 remain pending during initialization after earlier image pull failures.

Root filesystems are now **79–84% full**, significantly higher than the previous snapshot. `/mnt/disk1` usage is **25–31%**.

## Node inventory

All nodes are QEMU/KVM virtual machines running Ubuntu 22.04.3 LTS, kernel `5.15.0-161-generic`, on x86_64. Each has 4 vCPUs (Intel Xeon Silver 4314), approximately 15 GiB RAM, and no swap.

| Node | Address | Kubernetes role | RAM used / available | Root filesystem | `/mnt/disk1` | Uptime | Load average |
|---|---|---|---:|---:|---:|---:|---|
| Node1 (`team1-node1`) | `10.10.61.51` | Control plane | 10 GiB / 5.1 GiB | 25/32 GiB (**84%**) | 101/393 GiB (27%) | 33d 19h | 0.53, 0.87, 0.98 |
| Node2 (`team1-node2`) | `10.10.61.52` | Control plane | 10 GiB / 5.0 GiB | 25/32 GiB (**83%**) | 113/393 GiB (**31%**) | 33d 19h | 1.36, 0.89, 0.74 |
| Node3 (`team1-node3`) | `10.10.61.53` | Control plane | 10 GiB / 5.0 GiB | 24/32 GiB (**79%**) | 92/393 GiB (25%) | 33d 19h | 0.97, 0.82, 0.87 |
| Node4 (`team1-node4`) | `10.10.61.54` | Worker | 7.5 GiB / 7.8 GiB | 24/32 GiB (**81%**) | 96/393 GiB (26%) | 33d 19h | 0.32, 0.71, 0.76 |

### Network and virtual IPs

| Address | Observed owner or purpose |
|---|---|
| `10.10.61.50` | Keepalived VIP on Node4 |
| `10.10.61.51` | Node1 |
| `10.10.61.52` | Node2 |
| `10.10.61.53` | Node3 |
| `10.10.61.54` | Node4 |
| `10.10.61.60` | Keepalived VIP on Node2 |
| `10.10.61.61` | Keepalived VIP on Node3 |
| `10.10.61.70:5432` | PostgreSQL primary/write LoadBalancer endpoint |
| `10.10.61.71` | Spark master LoadBalancer endpoint |
| `10.10.61.72` | Spark history server LoadBalancer endpoint |

Keepalived was active on all four nodes. No additional VIP was observed on Node1 at snapshot time.

## Service placement

The table is based on live processes and listening sockets, not only service-unit names.

| Service | Node1 | Node2 | Node3 | Node4 |
|---|:---:|:---:|:---:|:---:|
| Kafka broker (`9092`) / controller (`9096`) | Yes | Yes | Yes | Yes |
| ZooKeeper (`2181`; quorum `2888/3888`) | No | Yes | Yes | Yes |
| HDFS NameNode (`8020`, `9870`) | Yes | Yes | Yes, **active observed** | No |
| HDFS DataNode (`9864`, `9866`, `9867`) | Yes | Yes | Yes | Yes |
| HDFS JournalNode (`8480`, `8485`) | Yes | Yes | Yes | No |
| HDFS ZKFC (`8019`) / HTTPFS (`14000`) | Yes | Yes | Yes | No |
| StarRocks FE (`8030`, `9010`, `9020`, `9030`) | Yes | Yes | Yes | Yes |
| StarRocks BE (`8040`, `8060`, `9050`, `9060`, `9070`) | Yes | Yes | Yes | Yes |
| Kubernetes control plane (`6443`, etcd `2379/2380`) | Yes | Yes | Yes | No |
| kubelet / kube-proxy | Yes | Yes | Yes | Yes |
| Spark worker listener (`7078`, `8081`) | No | Yes | No | Yes |
| Monitoring listeners (`3000`, `9090`) | No | No | No | Yes |
| Docker / containerd | Active | Active | Active | Active |

## Kubernetes

### Platform state

- Version: Kubernetes `v1.32.12`
- Runtime: containerd `2.1.5`
- Nodes: 4/4 `Ready`
- Control plane: Node1, Node2, Node3
- Worker-only node: Node4
- Namespace reviewed for application workloads: `team1`
- CoreDNS, Calico, etcd, API server, scheduler, controller manager, kube-proxy, MetalLB, and OpenEBS pods were observed running.

### Application workload state

| Workload | Placement | Ready state | Observed status |
|---|---|---:|---|
| PostgreSQL `postgresql-0` | Node4 | 2/2 | Running; current replica endpoint |
| PostgreSQL `postgresql-1` | Node2 | 2/2 | Running; current primary/write endpoint |
| PostgreSQL `postgresql-2` | Node1 | 0/2 | **ImagePullBackOff** |
| Spark master | Node4 | 1/1 | Running |
| Spark history server | Node4 | 1/1 | Running |
| Spark worker on Node4 | Node4 | 2/2 | Running |
| Spark worker on Node2 | Node2 | 2/2 | Running |
| Spark worker on Node3 | Node3 | 0/2 | **Pending / PodInitializing**; earlier `Init:ImagePullBackOff` observed |
| Spark worker on Node1 | Node1 | 0/2 | **Pending / PodInitializing**; earlier `Init:ImagePullBackOff` observed |
| ZooKeeper `0`, `1`, `2` | Node3, Node2, Node4 | 1/1 each | Running |
| Ostorlab | Node4 | 1/1 | Running; one restart observed |

### Services

| Service | Type | Cluster IP | External IP | Ports |
|---|---|---|---|---|
| `postgresql` | ClusterIP | `10.100.167.184` | — | 5432 |
| `postgresql-master` | LoadBalancer | `10.105.29.212` | `10.10.61.70` | 5432 |
| `postgresql-replica` | ClusterIP | `10.108.146.128` | — | 5432 |
| `patroni` | ClusterIP | `10.100.225.173` | — | 8000 |
| `spark-master` | LoadBalancer | `10.99.218.230` | `10.10.61.71` | 80, 7077, 6066, 9200 |
| `spark-spark-history-server` | LoadBalancer | `10.109.248.112` | `10.10.61.72` | 80 |
| `zookeeper` | ClusterIP | `10.103.100.29` | — | 2181 |
| `zookeeper-zookeeper-headless` | Headless | — | — | 2181, 2888, 3888 |
| `ostorlab-ostorlab` | ClusterIP | `10.110.21.126` | — | 80 |

### Persistent volume claims

All six reviewed claims were `Bound` using the `disk1` storage class.

| Claim | Capacity | Volume | Placement from workload topology |
|---|---:|---|---|
| `data-postgresql-postgresql-0` | 5 GiB | `pvc-128ed659-3970-4048-836e-4bf4cfe9cbe1` | Node4 |
| `data-postgresql-postgresql-1` | 5 GiB | `pvc-c89b1a1a-8bb4-45ad-af68-ceb44d1e6049` | Node2 |
| `data-postgresql-postgresql-2` | 5 GiB | `pvc-98093f1c-005d-4c73-bb18-0aae4ff81380` | Node1 |
| `zookeeper-zookeeper-zookeeper-0` | 5 GB | `pvc-3d7139e6-a607-45f5-93c4-198c6dd22283` | Node3 |
| `zookeeper-zookeeper-zookeeper-1` | 5 GB | `pvc-b0bfca8a-0bd9-401d-95a6-53b8377e0e0c` | Node2 |
| `zookeeper-zookeeper-zookeeper-2` | 5 GB | `pvc-0f65ba6d-ba27-44b2-87c1-8d4946a31ee4` | Node4 |

## HDFS

HDFS 3.3.6 was operational, out of safe mode, with four live DataNodes, zero dead or stale DataNodes, and zero volume failures.

| Metric | Current value |
|---|---:|
| Raw HDFS capacity | 1,517,776,316,008 bytes (about 1.38 TiB) |
| HDFS used | 163,856,971,484 bytes (about 152.6 GiB; 10.80%) |
| HDFS remaining | 1,171,303,432,192 bytes (about 1.07 TiB) |
| Non-HDFS used | 96,649,883,532 bytes (about 90.0 GiB) |
| Live / dead DataNodes | 4 / 0 |
| Total blocks / files | 2,376 / 2,220 |
| Missing, corrupt, or under-replicated blocks | 0 |
| Active NameNode observed | Node3 (`10.10.61.53:8020`, web UI `:9870`) |
| Journal quorum | Nodes 1–3 on port `8485` |

### DataNode distribution

| Node | HDFS capacity | HDFS used | Remaining | Used % | Blocks |
|---|---:|---:|---:|---:|---:|
| Node1 | 379,444,079,002 B | 35,439,292,416 B | 292,590,055,424 B | 9.34% | 1,583 |
| Node2 | 379,444,079,002 B | 50,343,088,128 B | 279,589,842,944 B | 13.27% | 2,295 |
| Node3 | 379,444,079,002 B | 38,774,988,800 B | 301,596,807,168 B | 10.22% | 1,642 |
| Node4 | 379,444,079,002 B | 39,299,602,140 B | 297,526,726,656 B | 10.36% | 1,608 |

### `/netflow` content summary

| Metric | Current value |
|---|---:|
| Directories | 5 |
| Files | 2,212 |
| Logical data length | 54,184,578,878 bytes (about 50.5 GiB) |
| Replicated space consumed | 162,553,736,634 bytes (about 151.4 GiB) |

The Hadoop CLI currently fails under the default JDK 17 because its environment supplies the removed JVM option `PrintGCTimeStamps`. The live values above were therefore read from the NameNode JMX and WebHDFS APIs. Running Hadoop processes were observed using the installed JDK 8 runtime.

## Kafka

All four servers were listening as Kafka brokers/controllers. The `netflow` topic was healthy at snapshot time: every replica was in the ISR and no partition showed an out-of-sync replica.

| Setting | Current value |
|---|---|
| Kafka version | 3.7.2 (Scala 2.13 distribution) |
| Topic | `netflow` |
| Partitions | 3 |
| Replication factor | 2 |
| Retention | `6048000000` ms (**70 days**) |
| Segment size | `1073741824` bytes (1 GiB) |
| Maximum message size | `3145728` bytes (3 MiB) |
| Retention byte limit | Unlimited (`-1`) |

| Partition | Leader | Replicas | In-sync replicas |
|---:|---|---|---|
| 0 | Node4 (`54`) | `54,51` | `51,54` |
| 1 | Node1 (`51`) | `51,52` | `51,52` |
| 2 | Node2 (`52`) | `52,53` | `52,53` |

Consumer groups visible at snapshot time:

- `netflow-consumer-deploy`
- `netflow-consumer-deploy-test`
- `netflow-starrocks-writer`
- `netflow-starrocks-writer2`
- `netflow-starrocks-writer3`
- `netflow-starrocks-writer4`
- `netflow-starrocks-writer5`

## Spark and batch processing

The Spark master API at `10.10.61.71` reported `ALIVE`, but only two workers were registered and alive:

| Worker | Cores | Memory | State |
|---|---:|---:|---|
| Node2 | 2 | 2 GiB | Alive, idle |
| Node4 | 2 | 2 GiB | Alive, idle |

Cluster total at snapshot time: 4 cores, 4 GiB worker memory, no active applications. Node1 and Node3 workers were unavailable; their pods moved from `Init:ImagePullBackOff` to `PodInitializing` during collection but remained 0/2 ready.

The master recorded four completed applications named `batch-processor` on 2026-09-05. Three used all four available cores; all four finished successfully. This confirms recent end-to-end submissions reached Spark, but it does not by itself validate output completeness.

## Installed platform versions

| Component | Observed version or installation |
|---|---|
| Ubuntu | 22.04.3 LTS |
| Linux kernel | 5.15.0-161-generic |
| Java default | 17.0.6 |
| Java for running Hadoop processes | JDK 8 (`/opt/jdk1.8.0_361`) |
| Hadoop | 3.3.6 |
| Kafka | 3.7.2 (`/opt/kafka_2.13-3.7.2`) |
| StarRocks | 4.0.12 (`/opt/StarRocks-4.0.12-ubuntu-amd64`) |
| Kubernetes | v1.32.12 |
| containerd | 2.1.5 |
| Docker | 26.1.3 |
| etcd installation | 3.5.21 |

Spark and PostgreSQL application versions were not re-derived from container images in this read-only snapshot; consult the current Kubernetes manifests or image metadata before relying on previously documented version labels.

## Current risks and recommended follow-up

1. **Investigate image pulls first.** Capture `kubectl describe pod` events for `postgresql-postgresql-2` and the Node1/Node3 Spark workers, then verify registry reachability, image names/tags, and image-pull secrets. Do not restart or redeploy until the cause is known.
2. **Reduce root filesystem pressure.** All root disks are at 79–84%. Identify the largest directories and growth sources with read-only disk-usage checks before deleting or rotating anything.
3. **Resolve the Hadoop Java mismatch.** Make the Hadoop CLI consistently select JDK 8 or update its JVM flags for JDK 17. Treat this as an operational tooling issue; HDFS itself was healthy.
4. **Verify PostgreSQL replication after restoring the replica.** The write endpoint is available, but one of the two replicas is not running.
5. **Re-check Spark capacity after image recovery.** Current effective capacity is half of the intended four-worker layout.

## Collection notes

- Collection was performed through SSH using read-only OS commands and read-only service APIs/CLI queries.
- Kubernetes inspection used `kubectl get`; no manifests or cluster resources were changed.
- No deployments, restarts, Docker operations, Kubernetes mutations, SCP transfers, or remote file writes were performed.
- Service-unit status alone was not treated as authoritative because some platform services use custom unit names; running processes and listening ports were cross-checked.

## Placement appendix: complete services and pods

### Snapshot and legend

| Field | Value |
|---|---|
| Snapshot time | 2026-09-06 08:35 IRST |
| Kubernetes scope | All namespaces visible to the `kubernetes` administrative user |
| Host scope | Read-only process, socket, and service checks on `10.10.61.51`–`10.10.61.54` |
| `RUN` | Present and running/ready |
| `DEG` | Present but degraded, pending, or not fully ready |
| `CP` | Kubernetes control-plane component |
| `—` | Not scheduled or not observed on that node |

### Consolidated node placement

| Service or role | Node1 `.51` | Node2 `.52` | Node3 `.53` | Node4 `.54` | Runtime / source |
|---|:---:|:---:|:---:|:---:|---|
| Kubernetes control plane | CP | CP | CP | — | Static pods: API server, etcd, scheduler, controller manager |
| Kubernetes worker services | RUN | RUN | RUN | RUN | kubelet, kube-proxy, containerd |
| Calico node networking | RUN | RUN | RUN | RUN | DaemonSet |
| CoreDNS | RUN | — | RUN | — | Deployment, 2 replicas |
| Calico Typha | RUN | — | — | — | Deployment, 1 replica |
| Calico controllers | — | — | RUN | — | Deployment, 1 replica |
| MetalLB controller | — | — | RUN | — | Deployment |
| MetalLB speaker | — | — | — | RUN | DaemonSet constrained to Node4 |
| OpenEBS LocalPV provisioner | — | — | — | RUN | Deployment |
| Ostorlab operator | — | — | — | RUN | Deployment |
| PostgreSQL instance | DEG | RUN, **primary** | — | RUN, replica | StatefulSet + Patroni |
| PostgreSQL operator | — | — | — | RUN | Deployment |
| Spark master | — | — | — | RUN | Deployment |
| Spark history server | — | — | — | RUN | Deployment |
| Spark worker | DEG | RUN | DEG | RUN | DaemonSet |
| ZooKeeper server | — | RUN (`1`) | RUN (`0`) | RUN (`2`) | StatefulSet with host networking |
| Kafka broker | RUN (`51`) | RUN (`52`) | RUN (`53`) | RUN (`54`) | Host service, port 9092 |
| Kafka KRaft controller | RUN | RUN | RUN | RUN | Host service, port 9096 |
| HDFS NameNode | RUN, HA | RUN, HA | RUN, **active** | — | Host service |
| HDFS DataNode | RUN | RUN | RUN | RUN | Host service |
| HDFS JournalNode | RUN | RUN | RUN | — | Host service |
| HDFS ZKFC / HTTPFS | RUN | RUN | RUN | — | Host service |
| StarRocks frontend | RUN | RUN | RUN | RUN | Host service |
| StarRocks backend | RUN | RUN | RUN | RUN | Host service |
| Prometheus | — | — | — | RUN | Host/container process, port 9090 |
| Grafana | — | — | — | RUN | Host/container process, port 3000 |
| Node Exporter | RUN | RUN | RUN | RUN | Host/container process |
| Jenkins build agents | RUN (2 processes) | — | — | — | Host Java processes (`remoting.jar`) |
| Docker daemon | RUN | RUN | RUN | RUN | Host service |
| Keepalived | RUN | RUN | RUN | RUN | Host service |

### Team1 Kubernetes application pods

| Controller / pod | Kind | Image(s) | Node | Pod IP | Ready | State |
|---|---|---|---|---|---:|---|
| `metallb-metallb-controller` | Deployment | `metallb/controller:v0.14.9` | Node3 `.53` | `10.244.54.6` | 1/1 | Running |
| `metallb-metallb-speaker` | DaemonSet | `metallb/speaker:v0.14.9` | Node4 `.54` | `10.10.61.54` | 1/1 | Running |
| `openebs-localpv-openebs-localpv` | Deployment | `openebs/provisioner-localpv:4.1.1` | Node4 `.54` | `10.244.25.147` | 1/1 | Running |
| `ostorlab-ostorlab` | Deployment | `zoraq/ostorlab-operator:1.0.2` | Node4 `.54` | `10.244.25.157` | 1/1 | Running |
| `postgresql-postgresql-0` | StatefulSet | `zoraq/patroni:3.3.6-1`, postgres-exporter `v0.15.0` | Node4 `.54` | `10.244.25.150` | 2/2 | Running; replica service backend |
| `postgresql-postgresql-1` | StatefulSet | `zoraq/patroni:3.3.6-1`, postgres-exporter `v0.15.0` | Node2 `.52` | `10.244.227.68` | 2/2 | Running; **primary/write backend** |
| `postgresql-postgresql-2` | StatefulSet | `zoraq/patroni:3.3.6-1`, postgres-exporter `v0.15.0` | Node1 `.51` | `10.244.233.111` | 1/2 | **Pending / ImagePullBackOff** |
| `postgresql-postgresql-operator` | Deployment | `zoraq/postgresql-operator:4.1.2` | Node4 `.54` | `10.244.25.148` | 1/1 | Running |
| `spark-spark-master` | Deployment | `zoraq/spark:4.1.1-1` | Node4 `.54` | `10.244.25.156` | 1/1 | Running |
| `spark-spark-history-server` | Deployment | `zoraq/spark:4.1.1-1` | Node4 `.54` | `10.244.25.155` | 1/1 | Running |
| `spark-spark-worker` | DaemonSet | `zoraq/spark:4.1.1-1`, `zoraq/ubuntu:jammy-20231128-2` | Node1 `.51` | `10.10.61.51` | 0/2 | **Pending / PodInitializing** |
| `spark-spark-worker` | DaemonSet | Same as above | Node2 `.52` | `10.10.61.52` | 2/2 | Running |
| `spark-spark-worker` | DaemonSet | Same as above | Node3 `.53` | `10.10.61.53` | 0/2 | **Pending / PodInitializing** |
| `spark-spark-worker` | DaemonSet | Same as above | Node4 `.54` | `10.10.61.54` | 2/2 | Running |
| `zookeeper-zookeeper-0` | StatefulSet | `zoraq/zookeeper:3.6.3-1` | Node3 `.53` | `10.10.61.53` | 1/1 | Running |
| `zookeeper-zookeeper-1` | StatefulSet | `zoraq/zookeeper:3.6.3-1` | Node2 `.52` | `10.10.61.52` | 1/1 | Running |
| `zookeeper-zookeeper-2` | StatefulSet | `zoraq/zookeeper:3.6.3-1` | Node4 `.54` | `10.10.61.54` | 1/1 | Running |

### Kubernetes system pods

| Component | Controller type | Node1 `.51` | Node2 `.52` | Node3 `.53` | Node4 `.54` | State |
|---|---|:---:|:---:|:---:|:---:|---|
| `calico-node` | DaemonSet | 1 | 1 | 1 | 1 | All running |
| `kube-proxy` | DaemonSet | 1 | 1 | 1 | 1 | All running |
| `calico-typha` | Deployment | 1 | — | — | — | Running |
| `calico-kube-controllers` | Deployment | — | — | 1 | — | Running |
| `coredns-coredns` | Deployment | 1 | — | 1 | — | Both running |
| `etcd` | Static pod | 1 | 1 | 1 | — | All running |
| `kube-apiserver` | Static pod | 1 | 1 | 1 | — | All running |
| `kube-controller-manager` | Static pod | 1 | 1 | 1 | — | All running |
| `kube-scheduler` | Static pod | 1 | 1 | 1 | — | All running |

### Kubernetes services and actual ready backends

| Namespace | Service | Type | Client endpoint | Ready backend node(s) | Backend address / port |
|---|---|---|---|---|---|
| `default` | `kubernetes` | ClusterIP | `10.96.0.1:443` | Nodes1–3 | `10.10.61.51–53:6443` |
| `kube-system` | `calico-typha` | ClusterIP | `10.106.222.194:5473` | Node1 | `10.10.61.51:5473` |
| `kube-system` | `kube-dns` | ClusterIP | `10.96.0.10:53` | Nodes1, 3 | `10.244.233.67`, `10.244.54.5` |
| `team1` | `ostorlab-ostorlab` | ClusterIP | `10.110.21.126:80` | Node4 | `10.244.25.157:8080` |
| `team1` | `patroni` | ClusterIP | `10.100.225.173:8000` | Node2 | `10.244.227.68:8000` |
| `team1` | `postgresql` | ClusterIP | `10.100.167.184:5432` | Node2 | `10.244.227.68:5432` |
| `team1` | `postgresql-master` | LoadBalancer | `10.10.61.70:5432` | Node2, **current primary** | `10.244.227.68:5432` |
| `team1` | `postgresql-replica` | ClusterIP | `10.108.146.128:5432` | Node4 | `10.244.25.150:5432` |
| `team1` | `spark-master` | LoadBalancer | `10.10.61.71` | Node4 | `10.244.25.156:6066/7077/8080/9200` |
| `team1` | `spark-spark-history-server` | LoadBalancer | `10.10.61.72:80` | Node4 | `10.244.25.155:18080` |
| `team1` | `zookeeper` | ClusterIP | `10.103.100.29:2181` | Nodes2–4 | `.52/.53/.54:2181` |
| `team1` | `zookeeper-zookeeper-headless` | Headless | Pod DNS | Nodes2–4 | `.52/.53/.54:2181/2888/3888` |
| `team1` | `postgresql-config` | Headless | Pod DNS | No EndpointSlice address | No service port |

### Kafka brokers and `netflow` replicas

| Broker ID | Node | Broker endpoint | Controller endpoint | Topic leadership |
|---:|---|---|---|---|
| 51 | Node1 `.51` | `10.10.61.51:9092` | `10.10.61.51:9096` | Leader for partition 1 |
| 52 | Node2 `.52` | `10.10.61.52:9092` | `10.10.61.52:9096` | Leader for partition 2 |
| 53 | Node3 `.53` | `10.10.61.53:9092` | `10.10.61.53:9096` | Replica for partition 2 |
| 54 | Node4 `.54` | `10.10.61.54:9092` | `10.10.61.54:9096` | Leader for partition 0 |

| `netflow` partition | Leader node | Replica nodes | In-sync nodes | Placement state |
|---:|---|---|---|---|
| 0 | Node4 `.54` | Node4 `.54`, Node1 `.51` | Node1, Node4 | Healthy |
| 1 | Node1 `.51` | Node1 `.51`, Node2 `.52` | Node1, Node2 | Healthy |
| 2 | Node2 `.52` | Node2 `.52`, Node3 `.53` | Node2, Node3 | Healthy |

### Hadoop and HDFS daemon placement

| HDFS daemon | Node1 `.51` | Node2 `.52` | Node3 `.53` | Node4 `.54` | Ports / role |
|---|:---:|:---:|:---:|:---:|---|
| NameNode | Running, HA peer | Running, HA peer | Running, **active observed** | — | RPC 8020, UI 9870 |
| DataNode | Running | Running | Running | Running | 9864, 9866, 9867 |
| JournalNode | Running | Running | Running | — | 8480, 8485 |
| ZKFC | Running | Running | Running | — | 8019 |
| HTTPFS | Running | Running | Running | — | 14000 |
| Data blocks | 1,583 | 2,295 | 1,642 | 1,608 | All four DataNodes in service |

### StarRocks placement

| StarRocks component | Node1 `.51` | Node2 `.52` | Node3 `.53` | Node4 `.54` | Observed ports |
|---|:---:|:---:|:---:|:---:|---|
| Frontend (FE) | Running | Running | Running | Running | 8030 HTTP, 9010 edit log, 9020 RPC, 9030 MySQL |
| Backend (BE) | Running | Running | Running | Running | 8040 HTTP/stream load, 8060 brpc, 9050 heartbeat, 9060 BE, 9070 |
| Version installation | 4.0.12 | 4.0.12 | 4.0.12 | 4.0.12 | `/opt/StarRocks-4.0.12-ubuntu-amd64` |

### Project runtime coverage

| Nimbo component | Expected data path | Dedicated Kubernetes controller/pod found | Matching persistent host Java process found | Observed execution model at snapshot |
|---|---|:---:|:---:|---|
| `parquet-writer` | Kafka → HDFS/Parquet | No | No | Not observed as a persistent Kubernetes or direct host process |
| `batch-processor` | HDFS/Parquet → Spark → PostgreSQL | No dedicated app pod | No | Submitted Spark applications; four completed jobs were visible from 2026-09-05 |
| `realtime` | Kafka → StarRocks | No | No | Not observed as a persistent Kubernetes or direct host process |
| `rest-api` | PostgreSQL/StarRocks query API | No | No | Not observed as a persistent Kubernetes or direct host process |
| Shared Avro/model code | Schema dependency used by application modules | Not applicable | Not applicable | Library/module, not a server service |
| Jenkins CI/CD | Builds and submissions | No Kubernetes pod | Yes, Node1 | Two Jenkins remoting-agent processes observed |

### Placement caveats

| Caveat | Meaning |
|---|---|
| PostgreSQL roles are dynamic | Pod ordinal `0` is not automatically the primary; the live `postgresql-master` EndpointSlice is the authority and routed to Node2 during this snapshot. |
| Spark worker pod names can change | The worker is a DaemonSet; placement by node is more stable than individual generated pod names. |
| Application absence is scoped | “Not observed” means no dedicated Kubernetes pod/controller or matching direct host process was present. Docker container inventory was not queried. |
| StarRocks membership role detail | FE/BE processes and ports were verified on every node; leader/follower FE metadata was not changed or inferred. |
| Snapshot values are time-sensitive | Patroni primary selection, pod names, pod states, IPs, and Kubernetes endpoints may change after failover or reconciliation. |
