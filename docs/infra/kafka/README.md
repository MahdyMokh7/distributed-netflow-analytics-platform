# Kafka (KRaft, split broker/controller) — Ansible role

Rebuilds the bare-metal Kafka cluster: 4 nodes, each running a dedicated
broker process (node.id = last octet of its IP: 51–54) and a dedicated
controller process (node.id 1–4), matching the topology your old
prometheus.yml was scraping (separate 7071 broker / 19096 controller JMX
ports per node).

## Before you run this

1. You need Ansible installed on your control machine (laptop or the
   Jenkins agent) with SSH access to team1-node1..4 as the `team1` user
   with sudo/become privileges.

2. **Cluster ID is already hardcoded** in `group_vars/all.yml`
   (`kafka_cluster_id: "Os2h_2loS1-CiwFAg9PTxg"`), per your mentor's
   go-ahead — you don't need to generate anything. Just don't change
   it after the first successful run, or already-formatted nodes will
   refuse to talk to freshly-formatted ones.

3. **Confirm the internal store download URL actually resolves.**
   `group_vars/all.yml` points at:
   ```
   https://store.inside.sahab.ir/repository/Raw-Hosted-Nebula/dist/kafka_2.13-4.2.1.tgz
   https://store.inside.sahab.ir/repository/Raw-Hosted-Nebula/dist/jmx_prometheus_javaagent-0.17.2.jar
   ```
   I derived `/repository/Raw-Hosted-Nebula/dist/...` from the standard
   Nexus3 raw-repo download convention, since the URL you were given
   (`.../#browse/browse:Raw-Hosted-Nebula:dist`) is the human browser
   view, not a direct download link. **Verify this from any node
   before running the playbook:**
   ```
   curl -I https://store.inside.sahab.ir/repository/Raw-Hosted-Nebula/dist/kafka_2.13-4.2.1.tgz
   ```
   If that 404s, open the browse URL in an actual browser, click into
   the file, and copy whatever URL it downloads from — then fix
   `internal_store_base_url` in `group_vars/all.yml` accordingly.

4. Java is **already installed** on all 4 nodes (confirmed: `java
   -version` → 17.0.6) — this role does not install or touch Java, it
   only verifies `java` is on PATH and fails loudly if not.

5. Confirm `ansible_user`/auth in `inventory.ini` matches how you
   already SSH to these nodes.

## Run it

```
ansible-playbook -i inventory.ini site.yml
```

`serial: 1` in site.yml brings nodes up one at a time on purpose — watch
each controller join the quorum before the next one starts, easier to
debug than all 4 racing at once.

## Verify

```
# On any node, check controller quorum health:
sudo -u kafka /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller 10.10.61.51:9093 describe --status

# Check brokers registered:
sudo -u kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.10.61.51:9092,10.10.61.52:9092,10.10.61.53:9092,10.10.61.54:9092

# Recreate your topics (they're gone — this was a full wipe):
sudo -u kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic multi-version-netflow --bootstrap-server 10.10.61.51:9092 \
  --partitions 6 --replication-factor 3
sudo -u kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic netflow-dlq --bootstrap-server 10.10.61.51:9092 \
  --partitions 6 --replication-factor 3

# Confirm JMX/Prometheus endpoints are alive (matches your old scrape config):
curl -s 10.10.61.51:7071/metrics | head
curl -s 10.10.61.51:19096/metrics | head
```

Once topics exist and metrics respond, your existing `prometheus.yml`
`kafka-brokers` / `kafka-controllers` jobs should start scraping
immediately with zero changes needed — the ports match exactly.

## Re-running safely (idempotency — the top priority this week)

Every task here is idempotent, specifically:
- **Storage formatting runs exactly once per process.** Each role checks
  for `<log.dir>/meta.properties` first (`kafka_broker_log_dir` /
  `kafka_controller_log_dir`) and only runs `kafka-storage.sh format`
  when that file doesn't exist yet. Once formatted, re-running the
  playbook will skip formatting entirely, even if you run it 50 times.
- Package download/extract use `creates:` guards — already-extracted
  Kafka is not re-downloaded or re-extracted.
- Config file renders are diff-based (Ansible's `template` module) —
  a file is only rewritten if its rendered content actually changed.
- Services only restart on config change (via `notify`/handlers), not
  on every run.

So: safe to re-run the whole playbook as many times as you want,
including mid-failure retries.
