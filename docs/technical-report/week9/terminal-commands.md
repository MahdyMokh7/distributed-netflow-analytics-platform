- `kubectl -n team1 exec -it postgresql-ha-postgresql-0 -- \
psql -h postgresql-ha-postgresql-master -U nimbo_user -d nimbo_db`


- `mysql -h 10.10.61.53 -P 9030 -u root -p`


- `kubectl logs -n team1 deployment/cube-maker -f`

- `kubectl logs -n team1 deployment/analytics-query -f`

- `kubectl logs -n team1 deployment/rest-api -f`