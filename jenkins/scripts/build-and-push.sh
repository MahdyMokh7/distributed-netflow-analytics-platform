#!/usr/bin/env bash
# Requires:
#   IMAGE_TAG, NEXUS_PUSH_REGISTRY, NEXUS_USERNAME, NEXUS_PASSWORD
#   MODULES_CSV            - comma-separated Maven module list to build, e.g. "rest-api,realtime"
#   BUILDS_CUBE_MAKER - "true"/"false" - whether cube-maker is in this run
set -eu
set +x

export XDG_CONFIG_HOME="${WORKSPACE}/.config"
export XDG_CACHE_HOME="${WORKSPACE}/.cache"
mkdir -p "${XDG_CONFIG_HOME}" "${XDG_CACHE_HOME}"

echo "Building release: ${IMAGE_TAG}"
echo "Modules this run: ${MODULES_CSV}"
echo "Pushing images to: ${NEXUS_PUSH_REGISTRY}"

mvn \
  --settings jenkins/maven-settings.xml \
  --batch-mode \
  --no-transfer-progress \
  -pl "${MODULES_CSV}" -am \
  clean package \
  -DskipTests

if [ "${BUILDS_CUBE_MAKER}" = "true" ]; then
    ./jenkins/scripts/validate-spark-jar.sh
fi

mvn \
  --settings jenkins/maven-settings.xml \
  --batch-mode \
  --no-transfer-progress \
  -pl "${MODULES_CSV}" \
  jib:build \
  -DskipTests \
  -Djib.allowInsecureRegistries=true \
  -Djib.image.tag="${IMAGE_TAG}" \
  -Djib.from.auth.username="${NEXUS_USERNAME}" \
  -Djib.from.auth.password="${NEXUS_PASSWORD}" \
  -Djib.to.auth.username="${NEXUS_USERNAME}" \
  -Djib.to.auth.password="${NEXUS_PASSWORD}" \
  -Djib.applicationCache="${XDG_CACHE_HOME}/jib-application" \
  -Djib.baseImageCache="${XDG_CACHE_HOME}/jib-base-image"