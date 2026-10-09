# Demo URL Cheat Sheet

| Service | URL |
|---|---|
| Gerrit — Nimbyte | [http://gerrit/q/project:Nimbyte](http://gerrit/q/project:Nimbyte) |
| Kafka UI | TODO: add Kafka UI URL |
| HDFS NameNode — node1 | [http://10.10.61.51:9870/](http://10.10.61.51:9870/) |
| HDFS Parquet Files — node1 | [http://10.10.61.51:9870/explorer.html#/netflow/parquetFiles](http://10.10.61.51:9870/explorer.html#/netflow/parquetFiles) |
| Spark Master UI | [http://10.10.61.71/](http://10.10.61.71/) |
| Spark History Server | [http://10.10.61.72/](http://10.10.61.72/) |
| Schema Registry | [http://10.10.61.77:8081/](http://10.10.61.77:8081/) |
| Schema Registry Subjects | [http://10.10.61.77:8081/subjects](http://10.10.61.77:8081/subjects) |
| Schema Registry — NetFlow schema v2 | [http://10.10.61.77:8081/subjects/multi-version-netflow-value/versions/2](http://10.10.61.77:8081/subjects/multi-version-netflow-value/versions/2) |
| pgAdmin | TODO: add pgAdmin URL |
| REST API Swagger | [http://10.10.61.75:18083/api/swagger-ui/index.html](http://10.10.61.75:18083/api/swagger-ui/index.html) |
| SonarQube — Nimbyte | [http://sonarqube.inside.sahab.ir/dashboard?id=nimbo-nimbyte](http://sonarqube.inside.sahab.ir/dashboard?id=nimbo-nimbyte) |
| Grafana | [http://10.10.61.74:3000/](http://10.10.61.74:3000/) |
| Grafana — Alerts | [http://10.10.61.74:3000/alerting/list](http://10.10.61.74:3000/alerting/list) |
| Prometheus | [http://10.10.61.73:9090/](http://10.10.61.73:9090/) |
| Prometheus Targets | [http://10.10.61.73:9090/targets](http://10.10.61.73:9090/targets) |
| Jenkins — CI | [http://darkoob:8080/view/Nimbo/job/nimbo-nimbyte-test/](http://darkoob:8080/view/Nimbo/job/nimbo-nimbyte-test/) |
| Jenkins — CD | [http://darkoob:8080/view/Nimbo/job/nimbo-nimbyte-release/](http://darkoob:8080/view/Nimbo/job/nimbo-nimbyte-release/) |
| Jenkins — Prepare | [http://darkoob:8080/job/nimbo-nimbyte-prepare/](http://darkoob:8080/job/nimbo-nimbyte-prepare/) |
| Jenkins — Backfill | [http://darkoob:8080/job/nimbo-nimbyte-backfill/](http://darkoob:8080/job/nimbo-nimbyte-backfill/) |
| Jenkins — Decommission | [http://darkoob:8080/job/nimbo-nimbyte-decommission/](http://darkoob:8080/job/nimbo-nimbyte-decommission/) |
| Analytics Query UI | [http://10.10.61.78/](http://10.10.61.78/) |


## PostgreSQL

```text
kubectl -n team1 exec -it postgresql-ha-postgresql-0 -- psql -h postgresql-ha-postgresql-master -U nimbo_user -d nimbo_db
```

## StarRocks Table

```bash
mysql -h 10.10.61.53 -P 9030 -u root -p \
  -e "SHOW CREATE TABLE nimbo.netflow\\G"
```

## Kubernetes Cluster

```bash
kubectl get nodes -o wide
kubectl get pods -n team1 -o wide
```
