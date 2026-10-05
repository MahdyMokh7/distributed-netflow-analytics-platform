# Nimbo Prometheus Infrastructure — Technical Report

Verified on **16 September 2026** against the repository configuration and the running `team1` Kubernetes cluster. This report distinguishes configured targets from metrics that Prometheus is currently collecting. No files or cluster resources were changed.

## 1. Architecture and scope

Prometheus runs as a single Kubernetes Deployment in namespace `team1`. It *pulls* metrics from applications and infrastructure every 30 seconds, stores time-series data on a persistent volume, and exposes its UI and HTTP API through the MetalLB address `10.10.61.73:9090`.

```text
Kubernetes application pods ── endpoint discovery ──┐
Spark master/workers ──────── Service/DNS discovery ──┤
PostgreSQL/Patroni ────────── StatefulSet pod DNS ────┤
Host-based Kafka, HDFS, node exporters, StarRocks ────┤
                                                      ▼
                                            Prometheus in team1
                                            30-second scraping
                                            20 GiB disk1 PVC
                                                      │
                                                      ▼
                                         10.10.61.73:9090
                                         UI, Targets, query API
```

The chart is [infra/helm-charts/prometheus](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/Chart.yaml). Its [scrape configuration](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/config/prometheus.yml) is embedded in a ConfigMap. Helm renders the release namespace into that configuration; Prometheus does not need separate hard-coded copies for each namespace.

This installation is a metrics collector and query server. The chart does **not** currently install Grafana dashboards, alert rules, or Alertmanager.

## 2. Prometheus deployment

| Concern | Configuration and reason |
| --- | --- |
| Workload | One Deployment replica. This is a single Prometheus server, not a highly available pair. |
| Image | `zoraq.inside.sahab.ir:9462/prometheus:latest`, `IfNotPresent`. The running binary reported version **3.14.0**. |
| Scrape interval | Global `30s`. Patroni also specifies `30s`, redundantly but harmlessly. |
| Retention | `15d`, subject to the available disk capacity. |
| Storage | `prometheus-data`, 20 GiB, `ReadWriteOnce`, OpenEBS `disk1`; mounted at `/prometheus`. |
| Resources | Request: 250m CPU/512 MiB. Limit: 1 CPU/2 GiB. |
| Exposure | `LoadBalancer` at `10.10.61.73:9090`; no Ingress is required. |
| Health | Kubernetes probes call `/-/ready` and `/-/healthy`. |
| Identity | Dedicated `prometheus` ServiceAccount and namespace-scoped, read-only discovery Role. |

These settings are in [values.yaml](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/values.yaml), [deployment.yaml](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/templates/deployment.yaml), [service.yaml](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/templates/service.yaml), and the [RBAC templates](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/templates/role.yaml).

The installed release has no user-supplied Helm value overrides, so it uses the chart defaults. The pod is running on **node4**. I checked its bound volume: the data directory is `/mnt/disk1/pvc-f45c6f0d-c86c-4c60-81e6-b824bbc1f093`, with node affinity to node4 and a `Retain` reclaim policy. This keeps metrics off the small root disk, but it is **node-local storage**: if node4 is unavailable, Kubernetes cannot simply attach this volume on another node. The Deployment uses `Recreate`, appropriate for its single `ReadWriteOnce` claim; it does not provide Prometheus HA. See the [PVC template](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/prometheus/templates/pvc.yaml).

A checksum of the rendered ConfigMap is placed on the pod template. A Helm upgrade that changes scrape configuration therefore rolls the Prometheus pod automatically. This is a restart, not a live configuration reload.

## 3. Why each discovery method was chosen

| Workloads | Method | Why it fits |
| --- | --- | --- |
| Parquet Writer, Batch Processor, Realtime, REST API | Namespace-scoped Kubernetes **endpoint discovery** | Their pod IPs change on rollout. Prometheus selects the exact Service name and the port named `http`, so replacement pods are discovered without editing IPs. |
| Spark master | Stable internal Kubernetes Service DNS | There is one master endpoint. Prometheus uses the cluster-internal `8080` UI/metrics port, not the public UI LoadBalancer. |
| Spark workers | DNS discovery of the headless worker Service | DNS returns each worker pod address. Prometheus refreshes it every 30 seconds, allowing workers to be replaced. |
| PostgreSQL exporter and Patroni | Three explicit StatefulSet pod DNS names | The ordinal identities `0`, `1`, and `2` are stable and match your choice for an easily auditable three-member cluster. |
| Kafka, HDFS, node exporters, StarRocks | Static node IPs and ports | These exporters are presented on known node addresses rather than through the four application Services. |

The PostgreSQL approach deliberately favors explicit targets over Kubernetes discovery. It is simple for the current fixed three replicas, but a replica-count or release-name change requires updating Prometheus. The names assume the PostgreSQL Helm release is `postgresql` in the same namespace; the live release confirms that assumption.

The Kubernetes discovery Role can read pods, Services, Endpoints, and EndpointSlices **within `team1` only**. It does not grant write access or cluster-wide discovery. The ServiceAccount token is mounted for that discovery; static and DNS targets do not themselves require Kubernetes API access.

## 4. Exporters and paths

| Job | Expected targets | Scrape path and exposure |
| --- | ---: | --- |
| `prometheus` | 1 | Self-scrape at `localhost:9090/metrics`. |
| `parquet-writer` | 1 pod by current chart | Service endpoint, `/actuator/prometheus`, port `18080`. |
| `batch-processor` | 1 pod by current chart | Service endpoint, `/actuator/prometheus`, port `18081`. |
| `realtime` | 1 pod by current chart | Service endpoint, `/actuator/prometheus`, port `18082`. |
| `rest-api` | 2 pods by current chart | Service endpoints, `/api/actuator/prometheus`, port `18083`. The `/api` prefix comes from its servlet context path. |
| `spark-master` | 1 | Internal `spark-master:8080`, `/metrics/master/prometheus/`. |
| `spark-workers` | 4 | Headless worker addresses on `8081`, `/metrics/prometheus/`. |
| `postgresql` | 3 | Per-pod postgres-exporter sidecar on `9187/metrics`. |
| `patroni` | 3 | Per-pod Patroni API on `8000/metrics`. |
| `kafka-brokers` | 4 | Nodes 1–4, JMX exporter on `7071/metrics`. |
| `kafka-controllers` | 3 | Nodes 2–4, JMX exporter on `19096/metrics`. |
| `hdfs-namenodes` | 3 | Nodes 1–3, `18020/metrics`. |
| `hdfs-datanodes` | 4 | Nodes 1–4, `19866/metrics`. |
| `node-exporters` | 4 | Nodes 1–4, `9100/metrics`. |
| `starrocks-fe` | 4 configured | Nodes 1–4, `8030/metrics`. |
| `starrocks-be` | 4 configured | Nodes 1–4, `8040/metrics`. |

All four application module POMs include Actuator and the Micrometer Prometheus registry. Their production chart configurations expose `prometheus`; their Service ports are named `http`, matching Prometheus’s endpoint filter. REST API needs its distinct path because of `/api`. The relevant configurations are the [Ingester](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/charts/parquet-writer/config/application-prod.yaml), [Processor](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/charts/batch-processor/config/application-prod.yaml), [Realtime](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/charts/realtime/config/application-prod.yaml), and [REST API](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/charts/rest-api/config/application-prod.yaml) production charts.

The [Spark chart](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/spark/templates/configmap.yaml) enables its Prometheus servlet and JVM metric sources. Its public master and History Server addresses (`.71` and `.72`) are for UI access; Prometheus scrapes the **internal** master and worker endpoints. The History Server is intentionally not a scrape target: its tested `/metrics/prometheus/` response was HTML, not Prometheus metrics. Spark master/worker metrics also should not be mistaken for every Batch Processor application-level metric; the Batch Processor’s own Actuator job serves that separate purpose.

Kafka’s [Ansible inventory](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/ansible/kafka/inventory.ini) places brokers on four nodes and controllers on nodes 2–4. Its systemd units start separate JMX exporter agents for [brokers](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/ansible/kafka/roles/kafka_broker/templates/kafka-broker.service.j2) and [controllers](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/ansible/kafka/roles/kafka_controller/templates/kafka-controller.service.j2). Removing the obsolete node1 controller target prevents a permanent false-down target.

PostgreSQL uses a [postgres-exporter sidecar in each StatefulSet pod](/home/mahdi.mokhtari/Desktop/nimbo/Nimbyte/infra/helm-charts/postgresql-ha/templates/statefulset.yaml), alongside Patroni’s own metrics API. These are complementary: exporter metrics describe PostgreSQL; Patroni metrics describe HA/leader state.

## 5. Live verification

At the time of this report, the Prometheus Deployment was `1/1` available, its Service had external IP `10.10.61.73`, and its 20 GiB PVC was bound. Its API reported:

| Job | `UP` / discovered |
| --- | ---: |
| Prometheus | 1/1 |
| Spark master / workers | 1/1 and 4/4 |
| PostgreSQL exporter / Patroni | 3/3 and 3/3 |
| Kafka brokers / controllers | 4/4 and 3/3 |
| HDFS NameNodes / DataNodes | 3/3 and 4/4 |
| Node exporters | 4/4 |
| StarRocks FE / BE | **4/4 and 0/4** |

That is **34 healthy of 38 currently discovered targets**. All four StarRocks BE targets refuse connections on `8040`; this is not a Prometheus startup or discovery failure. The four application Services were not deployed during verification, so their jobs had **no discovered targets**—that is different from a discovered target marked `DOWN`.

I also queried `scrape_samples_scraped`, rather than relying on HTTP or pod readiness alone. Each Spark master scrape contained 191 samples; workers 197 each; PostgreSQL exporters 525–543; Patroni 22; Kafka controllers 423–427. Thus Prometheus is ingesting data from the components specifically tested, not merely reaching their ports.

## 6. Important operational boundaries

- **External access is not access control.** The chart exposes Prometheus on `.73:9090` without configuring TLS, authentication, or `loadBalancerSourceRanges`. Access must be controlled by the surrounding network, or restrictions added before exposing it to a wider audience.
- **`latest` is mutable.** The running version is 3.14.0, but the Helm value does not identify an immutable build. Combined with `IfNotPresent`, a later rollout may reuse a locally cached image or fetch a different `latest` on another node. Pinning a tested tag or digest would make deployments reproducible; it is not required to explain the current scrape results.
- **Local persistence is not HA.** The PVC retains data on node4’s `/mnt/disk1`, but a node4 failure interrupts this single-replica server. `Retain` helps avoid automatic deletion; it is not a backup.
- **Configured does not mean monitored.** Missing application Services create no targets, and the down StarRocks BEs create failed targets. A complete monitoring rollout needs post-deployment target checks and, later, alerts for missing expected targets—not only alerts on `up == 0`.
- **Capacity needs observation.** Fifteen-day retention and a 20 GiB volume are configured, but actual disk consumption depends on series cardinality and the workloads deployed. Check TSDB/storage metrics after all applications are running.

## 7. How to inspect it

Open [Prometheus Targets](http://10.10.61.73:9090/targets) for per-endpoint health and scrape errors. On node1, these give the equivalent machine-readable checks:

```bash
curl -fsS 'http://10.10.61.73:9090/api/v1/targets?state=active' |
  jq -r '.data.activeTargets[] | [.labels.job, .health, .scrapeUrl, .lastError] | @tsv'
```

```bash
curl -fsSG 'http://10.10.61.73:9090/api/v1/query' \
  --data-urlencode 'query=scrape_samples_scraped{job=~"spark-master|spark-workers|postgresql|patroni|kafka-controllers"}' |
  jq -r '.data.result[] | [.metric.job, .metric.instance, .value[1]] | @tsv'
```

The immediate remaining verification is to repeat the target and sample checks after the four application releases are deployed, and to investigate StarRocks BE exposure separately if those backends are expected to serve metrics on `8040`.