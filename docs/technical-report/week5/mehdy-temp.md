### 1. ran fixed with change of datasource from Hikari to PGsimpledatasource then pass that to Hikari config


### 2. optimize batch size for writing to database


### 3. Read once of parquet-files in batch-processor (change architecture and now persist(memory_and_disk) each processor uses the persisted data instead of 8 reads)


### 4. fix all batch-processor unit and integration tests


### 5. remove the src_port, dest_port, L4_protocol from the table the cube and edited everything necessary so the cardinality decreases and i have less records per cube
