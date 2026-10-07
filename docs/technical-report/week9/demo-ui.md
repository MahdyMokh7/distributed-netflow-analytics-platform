# Demo URL Cheat Sheet

| Service | URL |
|---|---|
| Analytics Query UI | [http://10.10.61.78/](http://10.10.61.78/) |
| Analytics Query API | [http://10.10.61.78/api/analytics/query](http://10.10.61.78/api/analytics/query) |
| REST API Swagger | [http://10.10.61.75:18083/api/swagger-ui/index.html](http://10.10.61.75:18083/api/swagger-ui/index.html) |
| REST API OpenAPI JSON | [http://10.10.61.75:18083/api/api-docs](http://10.10.61.75:18083/api/api-docs) |
| SonarQube — Nimbyte | [http://sonarqube.inside.sahab.ir/dashboard?id=nimbo-nimbyte](http://sonarqube.inside.sahab.ir/dashboard?id=nimbo-nimbyte) |
| Grafana | [http://10.10.61.74:3000/](http://10.10.61.74:3000/) |
| Grafana — Current Storage | [http://10.10.61.74:3000/d/current-data-footprint/current-storage](http://10.10.61.74:3000/d/current-data-footprint/current-storage) |
| Grafana — Cube Maker | [http://10.10.61.74:3000/d/cube-maker/cube-maker-module](http://10.10.61.74:3000/d/cube-maker/cube-maker-module) |
| Grafana — Analytics Query | [http://10.10.61.74:3000/d/analytics-query/analytics-query-module](http://10.10.61.74:3000/d/analytics-query/analytics-query-module) |
| Grafana — Alerts | [http://10.10.61.74:3000/alerting/list](http://10.10.61.74:3000/alerting/list) |
| Spark Master UI | [http://10.10.61.71/](http://10.10.61.71/) |
| Spark History Server | [http://10.10.61.72/](http://10.10.61.72/) |
| HDFS NameNode — node1 | [http://10.10.61.51:9870/](http://10.10.61.51:9870/) |
| HDFS NameNode — node2 | [http://10.10.61.52:9870/](http://10.10.61.52:9870/) |
| HDFS NameNode — node3 | [http://10.10.61.53:9870/](http://10.10.61.53:9870/) |
| HDFS Parquet Files — node3 | [http://10.10.61.53:9870/explorer.html#/netflow/parquetFiles](http://10.10.61.53:9870/explorer.html#/netflow/parquetFiles) |
| Prometheus | [http://10.10.61.73:9090/](http://10.10.61.73:9090/) |
| Prometheus Targets | [http://10.10.61.73:9090/targets](http://10.10.61.73:9090/targets) |
| StarRocks FE — node1 | [http://10.10.61.51:8030/](http://10.10.61.51:8030/) |
| StarRocks FE — node2 | [http://10.10.61.52:8030/](http://10.10.61.52:8030/) |
| StarRocks FE — node3 | [http://10.10.61.53:8030/](http://10.10.61.53:8030/) |
| StarRocks FE — node4 | [http://10.10.61.54:8030/](http://10.10.61.54:8030/) |
| Schema Registry | [http://10.10.61.77:8081/](http://10.10.61.77:8081/) |
| Schema Registry Subjects | [http://10.10.61.77:8081/subjects](http://10.10.61.77:8081/subjects) |

## Dynamic Cube Maker Spark UI

```bash
kubectl get service cube-maker-spark-ui -n team1 \
  -o jsonpath='http://{.status.loadBalancer.ingress[0].ip}:{.spec.ports[0].port}{"\n"}'
```

## PostgreSQL

```text
postgresql://10.10.61.70:5432/nimbo_db
```
