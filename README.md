## Complete Flow: Build → Transfer → Run on Node3

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
```

**Note:** The `--add-opens` flags are **already configured** in your `pom.xml` `spring-boot-maven-plugin`, but when using `java -jar` directly, you must include them manually.

---

## Complete One-Liner (With JVM Flags)

On Node3:

```bash
java \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-opens=java.base/sun.net.util=ALL-UNNAMED \
  -jar batch-processor-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=prod
```

---

### Run as Background Service

```bash
nohup java [--add-opens flags] -jar batch-processor-1.0.0-SNAPSHOT.jar --spring.profiles.active=prod > batch-processor.log 2>&1 &

# Check logs
tail -f batch-processor.log
```

---

## Quick Script (Create on Node3)

Create `run-prod.sh`:

```bash
#!/bin/bash
JAVA_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED ..."
java $JAVA_OPTS -jar batch-processor-1.0.0-SNAPSHOT.jar --spring.profiles.active=prod

mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

Then:

```bash
chmod +x run-prod.sh
./run-prod.sh
```

---

## Monitor

- **Spark UI:** `http://10.10.61.71/`
- **Spark History UI:** `http://10.10.61.72/`
- **Logs:** `tail -f batch-processor.log`

spark-ui ip : http://10.10.61.71/
spark-ui history ip : http://10.10.61.72/