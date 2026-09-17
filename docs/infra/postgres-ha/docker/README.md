# Our own Postgres+Patroni image (not the infra team's)

## What's in it, and why (for defending this to your mentors)

Plain `postgres:16-alpine` + Patroni 3.3.6 (PyPI) + `psycopg2-binary`
(the Postgres driver Patroni actually needs to run at all — missing in
an earlier attempt, which is why `patroni --version` failed). No
TimescaleDB, no WAL-G, no PostGIS, no Persian locale — dropped
deliberately, not by accident. Full reasoning for every choice is in
the `Dockerfile` itself as comments, not just here — read those before
a mentor asks.

**One thing still worth doing once, not yet done:** grep
`batch-processor`'s Flyway migration SQL files for
`CREATE EXTENSION timescaledb`. If any migration assumes it exists,
dropping TimescaleDB breaks app startup on a fresh cluster. If nothing
turns up, this decision is fully settled.

## Step by step, start to finish

### 1. Clean up local leftovers from the last attempt (optional but tidy)

```bash
docker images | grep nimbo-postgres-patroni
# remove the broken v1 tags — must include the exact tag, or Docker
# assumes :latest and finds nothing:
docker rmi zoraq.inside.sahab.ir:9462/nimbo-postgres-patroni:16-patroni3.3.6-v1
docker rmi zoraq.inside.sahab.ir:9466/nimbo-postgres-patroni:16-patroni3.3.6-v1
```
(`:9462` and `:9466` were two tags pointing at the exact same broken
image — `docker tag` never copies anything, it just adds a second name
to the same image ID. Removing both tags removes it from your local
Docker cache; it does **not** touch anything already pushed to Nexus.)

### 2. Build v2

```bash
cd docker/postgres-patroni
docker build -t zoraq.inside.sahab.ir:9466/nimbo-postgres-patroni:16-patroni3.3.6-v2 .
```

### 3. Test locally, BEFORE pushing anything

```bash
docker run --rm zoraq.inside.sahab.ir:9466/nimbo-postgres-patroni:16-patroni3.3.6-v2 patroni --version
# expect: patroni 3.3.6

docker run --rm zoraq.inside.sahab.ir:9466/nimbo-postgres-patroni:16-patroni3.3.6-v2 psql --version
# expect: psql (PostgreSQL) 16.x
```

If either of these fails, stop — fix it before pushing. No point
putting a broken image on the registry a second time.

### 4. Push to the HOSTED port (:9466 — the only one that accepts writes)

```bash
docker push zoraq.inside.sahab.ir:9466/nimbo-postgres-patroni:16-patroni3.3.6-v2
```

### 5. `values.yaml` already points at the right thing

Already set to:
```yaml
image:
  patroni: "zoraq.inside.sahab.ir:9462/nimbo-postgres-patroni:16-patroni3.3.6-v2"
```
Note the **port switches back to `:9462`** here — that's not a typo.
You push to `:9466` (hosted), the cluster pulls from `:9462` (group),
same split your existing app images already use. Just double check the
tag (`v2`) matches exactly what you pushed.

### 6. Confirm a node can actually pull it, before touching Helm

```bash
ssh team1@10.10.61.51 "sudo crictl pull zoraq.inside.sahab.ir:9462/nimbo-postgres-patroni:16-patroni3.3.6-v2"
```

If this fails with `unauthorized`, that's a registry pull-permission
question (different from the push problem you already solved) — not
something to debug by guessing, ask whoever administers Nexus.

### 7. Only after all 6 steps pass — proceed to `helm install`

Back in the main chart README for the actual install/verify sequence.

## Cleaning up the old, broken v1 from Nexus itself

`docker rmi` (step 1) only ever touches your **local** machine's Docker
cache — it has no effect on what's sitting in Nexus. If a `v1` push did
succeed at some point and you want it gone from the registry too,
that's a Nexus-side operation (its web UI: Browse → the hosted
repository → find the `v1` component → delete; or its REST API), not a
`docker` command. Worth doing once `v2` is confirmed working end-to-end
— not urgent, and not worth doing before you've verified `v2` actually
works, in case you need to fall back.
