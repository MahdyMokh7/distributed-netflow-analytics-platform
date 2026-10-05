### 1. ran fixed with change of datasource from Hikari to PGsimpledatasource then pass that to Hikari config


### 2. optimize batch size for writing to database


### 3. Read once of parquet-files in batch-processor (change architecture and now persist(memory_and_disk) each processor uses the persisted data instead of 8 reads)


### 4. fix all batch-processor and rest-api unit and integration tests


### 5. remove the src_port, dest_port, L4_protocol from the table the cube and edited everything necessary so the cardinality decreases and i have less records per cube


### 6. add HDFS hourly partitioning for faster file discovery on parquet files


### 7. fix the mounted spark worker filesystem 

- Fix the Spark worker's storage location by correcting the LOCAL_DIRS_BASE configuration from `/mnt//mnt/disk1` to `/mnt/disk1`. 
- This ensures Spark uses the SSD-backed `/mnt/disk1/spark` directory for cache, shuffle, and executor working files, instead of the root disk (`/tmp/spark` and `/opt/spark/work`).

### 8. add Prometheus metrics


### 9. add Grafana dashboard


### 10. add extension to rest-api, batch-processor, parquet-writer


### 11. update helm charts for kubernetes batch-processor deployment