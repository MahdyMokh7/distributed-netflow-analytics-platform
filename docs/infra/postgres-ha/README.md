# Postgres HA (Patroni), our own image, no operator

## Latest round of changes

- **Dropped TimescaleDB entirely** (was originally kept for a reasoned,
  query-pattern-based justification — see git history if curious — but
  the team decided against it). Image is now plain `postgres:16-alpine` +
  Patroni. **One check still worth doing**: grep `batch-processor`'s
  Flyway migrations for `CREATE EXTENSION timescaledb` — if none exist,
  this decision is fully settled.
- **Fixed a real bug in the image**: `v1` had no Postgres driver
  installed at all — Patroni couldn't even report its own version. `v2`
  adds `psycopg2-binary` (the driver Patroni's own error message
  recommends first). Tag bumped to `v2` accordingly — never silently
  overwrite a tag that might already be pulled/cached somewhere.
- **Removed `exporter-queries.yml` entirely.** The two things actually
  needed — "is it up" and "who is master" — are both built into
  `postgres_exporter` by default (`pg_up`, `pg_replication_is_replica`).
  No custom query file needed, so it's gone, not just trimmed.
- **`appDatabase.existingSecretName` now points at the real, confirmed
  secret** (`batch-processor-secret`, `DB_USERNAME=nimbo_user`,
  `DB_PASSWORD=123456`) instead of a guessed name.
- **Removed Persian locale (`icu-locale: fa-IR`) from `patroni.yml`'s
  `initdb` block** — leftover from when the image still had that locale
  installed. Our own image doesn't include it, so the config shouldn't
  request it either.
- **Dockerfile explains version choices in the file itself**, not just
  in chat — every pin (pg16, Patroni 3.3.6, psycopg2-binary) and every
  deliberate non-pin (python3, su-exec) has its reasoning inline.

## Do this first — build your own image

`values.yaml` points at `zoraq.inside.sahab.ir:9462/nimbo-postgres-patroni:...`
— **this image doesn't exist until you build and push it.** Full,
step-by-step instructions including the registry's two-port split
(`:9466` to push, `:9462` to pull): `docker/postgres-patroni/README.md`.

That image is built from a public base (`postgres:16-alpine` + Patroni
from PyPI) — **not** the infra team's `zoraq/patroni`, per your mentor's
instruction.

## The operator is gone

Removed entirely, per your mentor — not toggled off, deleted. The 4
`zoraq.ir` CRDs stay inert in the cluster (they already existed
independently of this chart; nothing here created or removes them).
`templates/bootstrap-job.yaml` does the one thing that actually needs
doing — making sure `nimbo_db` exists — in plain, readable SQL. It
deliberately does **not** create the actual netflow table/schema:
`batch-processor` already owns that via Flyway migrations
(`spring.flyway.enabled: true`), so schema belongs there, not duplicated
here.

## Fixes from the review pass (kept from before)

1. **Passwords in a Secret**, not the ConfigMap — Patroni reads
   `PATRONI_SUPERUSER_PASSWORD` etc as env var overrides.
   **Change the placeholder passwords in `values.yaml`** —
   `CHANGE_ME_generate_with_openssl_rand` isn't a real password, generate
   one with `openssl rand -base64 24` for each of the 3.
2. **Resource `requests`** (no limits) on Postgres + exporter — the old
   `resources: {}` gave these pods BestEffort QoS, first evicted under
   memory pressure. Wrong default for a database.
3. **`pg_hba` left at `0.0.0.0/0`, deliberately** — your Calico pod-CIDR
   isn't confirmed anywhere, and guessing risks locking out your own
   apps. `templates/networkpolicy.yaml` is the textbook-correct
   restriction layer instead, opt-in (`networkPolicy.enabled: false` by
   default) — turn on once you've confirmed your namespace's network
   posture.

## The `.70`/`.71` MetalLB explanation, restated

Neither is a node (nodes are `.51`–`.54`). They're **MetalLB pool IPs** —
whichever node currently hosts a `LoadBalancer` Service's endpoint
answers ARP for its pool IP; that can move between nodes. MetalLB itself
survived Stone Age week, so the pool should still be intact — confirm
with `kubectl get ipaddresspools -n team1` before setting
`service.masterLoadBalancerIP` to `10.10.61.70` explicitly (left blank =
auto-assign a fresh one, which means updating `rest-api`/
`batch-processor`'s hardcoded connection strings afterward).

## What this deploys, in dependency order

1. **Secret** — the 3 passwords (change the placeholders first).
2. **RBAC + ServiceAccount** — Patroni can't elect a leader without
   permission to read/write Endpoints objects.
3. **ConfigMap** — `patroni.yml` (usernames only, no passwords) +
   `exporter-queries.yml`.
4. **StatefulSet**, 3 replicas, hard pod anti-affinity (1/node). Pod 0
   bootstraps; pods 1–2 join as replicas automatically via the
   Kubernetes-Endpoints DCS.
5. **4 Services** — `postgresql` (any pod), `postgresql-master`
   (LoadBalancer, always current primary), `postgresql-replica`, `patroni`
   (REST API, :8000).
6. **Bootstrap Job** — creates `nimbo_db` if it doesn't exist, nothing more.
7. **NetworkPolicy** (opt-in, off by default).

## Install

```
helm install postgresql ./postgresql-ha -n team1
```

## Verify, in order

```bash
# 1. Watch pods come up
kubectl get pods -n team1 -l cluster-name=postgresql -w

# 2. Confirm Patroni agrees on topology
kubectl exec -n team1 postgresql-postgresql-0 -c postgresql -- \
  patronictl -c /etc/patroni/patroni.yml list

# 3. Confirm master Service tracks the current leader
kubectl get endpoints postgresql-postgresql-master -n team1

# 4. Confirm the bootstrap Job created the database
kubectl logs -n team1 job/postgresql-postgresql-bootstrap

# 5. Demo-worthy: prove failover actually works
kubectl exec -n team1 postgresql-postgresql-0 -c postgresql -- \
  patronictl -c /etc/patroni/patroni.yml switchover
```

## Still open

- `storage.size` (`20Gi`) is a placeholder — no PVC survived to confirm
  the real prior size.
- **TimescaleDB is no longer in this image at all.** Confirm
  `batch-processor`'s Flyway migrations never run
  `CREATE EXTENSION timescaledb` — if one does, app startup will fail on
  a fresh cluster (the extension genuinely won't exist to create). One
  `grep -R timescaledb` through the migration SQL settles this either way.
- Verify `rest-api-secret` holds the same `DB_USERNAME`/`DB_PASSWORD` as
  `batch-processor-secret` (the one the bootstrap Job actually uses) —
  if they differ, `rest-api` won't be able to log in after install.
