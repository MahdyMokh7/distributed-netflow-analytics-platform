#!/usr/bin/env bash
# Usage: helm-deploy.sh <dry-run|apply>
# Requires:
#   UMBRELLA_CHART, KUBE_NAMESPACE, DEPLOY_ARGS (the --set ... --reuse-values string)
set -eu

MODE="${1:?usage: helm-deploy.sh <dry-run|apply>}"

if [ "${MODE}" = "dry-run" ]; then
    # shellcheck disable=SC2086
    helm upgrade --install nimbo-platform "${UMBRELLA_CHART}" \
      --namespace "${KUBE_NAMESPACE}" \
      ${DEPLOY_ARGS} \
      --dry-run=server
else
    # shellcheck disable=SC2086
    helm upgrade --install nimbo-platform "${UMBRELLA_CHART}" \
      --namespace "${KUBE_NAMESPACE}" \
      ${DEPLOY_ARGS} \
      --atomic \
      --wait \
      --timeout 10m
fi