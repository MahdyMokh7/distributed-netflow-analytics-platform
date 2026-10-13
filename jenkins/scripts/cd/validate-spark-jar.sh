#!/usr/bin/env bash

set -eu

echo "Validating Spark JAR..."

SPARK_JAR_COUNT="$(find cube-maker/target -maxdepth 1 -type f -name 'cube-maker-*-spark.jar' | wc -l)"

test "${SPARK_JAR_COUNT}" -eq 1

SPARK_JAR="$(find cube-maker/target -maxdepth 1 -type f -name 'cube-maker-*-spark.jar')"

test -s "${SPARK_JAR}"

SPARK_JAR_ENTRIES="$(jar tf "${SPARK_JAR}")"

for REQUIRED_CLASS in \
  com/nimbo/cubemaker/CubeMakerApplication.class \
  com/nimbo/cubemaker/writer/PartitionWriter.class \
  org/postgresql/Driver.class
do
    if ! printf '%s\n' "${SPARK_JAR_ENTRIES}" | grep -Fx "${REQUIRED_CLASS}" > /dev/null; then
        echo "Invalid Spark JAR: missing ${REQUIRED_CLASS}" >&2
        exit 1
    fi
done


if printf '%s\n' "${SPARK_JAR_ENTRIES}" | grep '^BOOT-INF/' > /dev/null; then
    echo "Invalid Spark JAR: Spring Boot nested layout detected" >&2
    exit 1
fi

echo "Spark JAR validation passed."
