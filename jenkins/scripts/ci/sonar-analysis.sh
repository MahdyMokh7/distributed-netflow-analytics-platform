#!/usr/bin/env bash
set -eu

mkdir -p /tmp/sonar-cache

if [ "${GERRIT_REFSPEC:-}" = "refs/heads/master" ]; then
    echo "Running Sonar analysis for the master branch"
    set -- "-Dsonar.branch.name=master"
else
    CHANGE="${GERRIT_CHANGE_NUMBER:-manual-build}"
    PATCHSET="${GERRIT_PATCHSET_NUMBER:-0}"
    echo "Running Sonar pull-request analysis for ${CHANGE}"
    set -- \
      "-Dsonar.pullrequest.key=${CHANGE}" \
      "-Dsonar.pullrequest.branch=gerrit-${CHANGE}-${PATCHSET}"
fi

mvn \
  --settings jenkins/maven-settings.xml \
  --batch-mode \
  --no-transfer-progress \
  org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
  -Dsonar.projectKey=nimbo-nimbyte \
  -Dsonar.projectName=nimbo-nimbyte \
  -Dsonar.host.url="${SONAR_HOST_URL}" \
  -Dsonar.login="${SONAR_AUTH_TOKEN}" \
  -Dsonar.userHome=/tmp/sonar-cache \
  -Dsonar.coverage.jacoco.xmlReportPaths="**/target/site/jacoco/jacoco.xml" \
  "$@"
