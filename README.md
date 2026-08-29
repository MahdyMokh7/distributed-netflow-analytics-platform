## Run Batch-Processor  [Complete Flow: Build → Transfer → Run on Node3]

### 1. Build JAR (Local Machine)

```bash
cd ~/Desktop/nimbo/Nimbyte/batch-processor

# Clean and package with tests skipped
mvn clean package -DskipTests
```

JAR location: `target/batch-processor-1.0.0-SNAPSHOT.jar`

---

### 2. Transfer JAR to Node3 (Local Machine)

```bash
scp target/batch-processor-1.0.0-SNAPSHOT.jar team1@10.10.61.53:~/
```

---

### 3. Run on Node3 (Server)

```bash
ssh team1@10.10.61.53

# Run with prod profile
java -jar batch-processor-1.0.0-SNAPSHOT.jar --spring.profiles.active=prod

java -jar batch-processor-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=prod \
  --batch.schedule="0 */1 * * * ?"
  
  
kubectl exec -it -n team1 spark-spark-master-7b9bb87b78-ntjn4 -- \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.nimbo.batch.BatchProcessorApplication \
  /tmp/batch-processor.jar \
  --spring.profiles.active=dev
  
  
docker run -it --rm \
  --network host \
  -v /home/team1/batch-processor-1.0.0-SNAPSHOT.jar:/app/batch-processor.jar \
  zoraq/spark:4.1.1 \
  spark-submit \
  --master spark://10.10.61.71:7077 \
  --deploy-mode client \
  --class com.nimbo.batch.BatchProcessorApplication \
  /app/batch-processor.jar \
  --spring.profiles.active=prod
  
  
mvn clean spring-boot:run -Dspring-boot.run.profiles=dev


docker run -d   --name nimbo-postgres   -e POSTGRES_USER=nimbo_user   -e POSTGRES_PASSWORD=123456   -e POSTGRES_DB=nimbo_db   -p 5432:5432   postgres:16.2
docker exec -it nimbo-postgres psql -U nimbo_user -d nimbo_db

```

## Monitor

- **Spark UI:** `http://10.10.61.71/`
- **Spark History UI:** `http://10.10.61.72/`
- **Logs:** `tail -f batch-processor.log`

spark-ui ip : http://10.10.61.71/
spark-ui history ip : http://10.10.61.72/



## How to Edit on PostgreSQL DB

### Clean path from your local system to `nimbo_db` as `nimbo_batch`

Use **Node1** because your `kubectl` access works there:

```bash
# 1. From your local system
ssh team1@10.10.61.51

# 2. Switch to the Kubernetes user
sudo su kubernetes

# 3. Go to the kubernetes user's home (optional)
cd /home/kubernetes

# 4. Enter the PostgreSQL master pod
kubectl exec -it -n team1 postgresql-postgresql-0 -- \
  psql -U postgres -d nimbo_db
```

You should then see:

```text
nimbo_db=>
```

Then, inside `psql`, you can create tables:

```sql
CREATE TABLE example (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

\dt
```

Also, **`psql -h ...` is a Linux command**, so don't type it inside the `nimbo_db=#` prompt. Inside `psql`, use SQL commands such as `CREATE TABLE`, or `\dt`, `\q`, etc.

## PostgreSQL / `psql` Quick Guide

### Connect

```bash
psql -h HOST -p 5432 -U USER -d DATABASE
```

Example:

```bash
psql -h 10.10.61.70 -p 5432 -U nimbo_user -d nimbo_db
```

### Tables

```sql
\dt                         -- list tables
\d table_name               -- describe table
\d+ table_name              -- detailed description
```

### Create table

```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Insert

```sql
INSERT INTO users (name)
VALUES ('Mahdi');
```

### Select

```sql
SELECT * FROM users;
SELECT id, name FROM users;
SELECT * FROM users WHERE id = 1;
```

### Update

```sql
UPDATE users
SET name = 'Ali'
WHERE id = 1;
```

**Always use `WHERE` unless you intentionally want to update every row.**

### Delete

```sql
DELETE FROM users
WHERE id = 1;
```

### Alter table

```sql
ALTER TABLE users ADD COLUMN email VARCHAR(255);

ALTER TABLE users DROP COLUMN email;

ALTER TABLE users RENAME COLUMN name TO full_name;

ALTER TABLE users RENAME TO customers;
```

### Drop table

```sql
DROP TABLE users;
```

Safer:

```sql
DROP TABLE IF EXISTS users;
```

### Databases

```sql
\l                          -- list databases
\c database_name            -- connect to another database
```

### Users / roles

```sql
\du                         -- list users/roles
```

Create:

```sql
CREATE USER myuser WITH PASSWORD 'password';
```

Change password:

```sql
ALTER USER myuser WITH PASSWORD 'new_password';
```

### Useful information

```sql
SELECT current_user;        -- current user
SELECT current_database();  -- current database
SELECT version();           -- PostgreSQL version
```

### Quit PostgreSQL

```sql
\q
```

### Clear terminal

```sql
\! clear
```

### Transactions — very useful

```sql
BEGIN;

UPDATE users SET name = 'Test' WHERE id = 1;

ROLLBACK;       -- undo changes
```

Or:

```sql
BEGIN;

UPDATE users SET name = 'Test' WHERE id = 1;

COMMIT;         -- permanently apply
```

### Important distinction

Commands starting with `\` are **psql commands**:

```text
\dt
\du
\l
\q
```

SQL commands are things like:

```sql
SELECT ...
CREATE TABLE ...
INSERT ...
UPDATE ...
DELETE ...
ALTER TABLE ...
```

And **don't type `psql ...` while already inside `psql`**. `psql` is the Linux command used to enter PostgreSQL in the first place.
