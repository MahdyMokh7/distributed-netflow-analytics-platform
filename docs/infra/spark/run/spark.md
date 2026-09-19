kubectl logs -f rest-api-9f7df9bcb-d68tw -n team1

kubectl -n team1 exec -it postgresql-postgresql-0 -c postgresql --   psql -h postgresql-postgresql-master -p 5432 -U nimbo_user -d nimbo_db -W