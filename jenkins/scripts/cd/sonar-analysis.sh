#!/usr/bin/env bash
set -eu

mkdir -p /tmp/sonar

mvn \
  --settings jenkins/maven-settings.xml \
  --batch-mode \
  --no-transfer-progress \
  clean verify

echo "===== JaCoCo reports ====="
find . -name "jacoco.xml" -print

mvn \
  --settings jenkins/maven-settings.xml \
  --batch-mode \
  --no-transfer-progress \
  org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
  -Dsonar.projectKey=nimbo-nimbyte \
  -Dsonar.projectName=nimbo-nimbyte \
  -Dsonar.branch.name=master \
  -Dsonar.host.url="${SONAR_HOST_URL}" \
  -Dsonar.login="${SONAR_AUTH_TOKEN}" \
  -Dsonar.userHome=/tmp/sonar \
  -Dsonar.coverage.jacoco.xmlReportPaths="**/target/site/jacoco/jacoco.xml"
