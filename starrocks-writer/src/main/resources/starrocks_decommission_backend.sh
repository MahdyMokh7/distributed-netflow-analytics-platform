#!/usr/bin/env bash
set -euo pipefail

FE_HOST="${FE_HOST:-10.10.61.51}"
FE_PORT="${FE_PORT:-9030}"
MYSQL_USER="${MYSQL_USER:-root}"
MAX_REPLICATION_FACTOR="${MAX_REPLICATION_FACTOR:-2}"
POLL_INTERVAL="${POLL_INTERVAL:-30}"
SSH_CONNECT_TIMEOUT="${SSH_CONNECT_TIMEOUT:-10}"

die() {
    echo "ERROR: $*" >&2
    exit 1
}

echo "StarRocks Backend Decommission Tool"
echo "==================================="

read -rsp "StarRocks root password: " MYSQL_PASSWORD
echo


read -rp "Backend address (IP:heartbeat_port): " BE
if [[ ! "$BE" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5}$ ]]; then
    die "Invalid backend address. Expected IPv4:heartbeat_port"
fi
BE_HOST="${BE%:*}"
BE_HEARTBEAT_PORT="${BE##*:}"

echo
echo "Using FE: ${FE_HOST}:${FE_PORT}"
echo "Target BE: ${BE}"
echo

mysql_cmd() {
    MYSQL_PWD="$MYSQL_PASSWORD" mysql \
        --connect-timeout=10 \
        -h "$FE_HOST" \
        -P "$FE_PORT" \
        -u "$MYSQL_USER" \
        -N -B \
        -e "$1"
}

get_backends() {
    mysql_cmd "SHOW BACKENDS;"
}

echo "[1/6] Checking backend exists..."
BACKENDS="$(get_backends)" ||
    die "Cannot query StarRocks backend list"

TARGET_ROW="$(
    awk -F '\t' \
        -v ip="$BE_HOST" \
        -v port="$BE_HEARTBEAT_PORT" \
        '$2 == ip && $3 == port {print}' \
        <<< "$BACKENDS"
)"

if [[ -z "$TARGET_ROW" ]]; then
    die "Backend $BE not found"
fi

echo "Backend found:"
echo "$TARGET_ROW"
echo

TARGET_ALIVE="$(awk -F '\t' '{print $9}' <<< "$TARGET_ROW")"
TARGET_DECOMMISSIONED="$(awk -F '\t' '{print $10}' <<< "$TARGET_ROW")"

[[ "$TARGET_ALIVE" == "true" ]] ||
    die "Backend is not alive"

[[ "$TARGET_DECOMMISSIONED" != "true" ]] ||
    die "Backend already decommissioned"



echo "[2/6] Checking remaining healthy backends..."


HEALTHY_REMAINING="$(
    awk -F '\t' \
        -v ip="$BE_HOST" \
        -v port="$BE_HEARTBEAT_PORT" '
        $2 != ip || $3 != port {
            if ($9=="true" && $10!="true" && $11!="true")
                count++
        }
        END {print count+0}
    ' <<< "$BACKENDS"
)"


echo "Healthy remaining backends: $HEALTHY_REMAINING"

if (( HEALTHY_REMAINING < MAX_REPLICATION_FACTOR )); then
    die "Not enough healthy BEs remain. Required=$MAX_REPLICATION_FACTOR Available=$HEALTHY_REMAINING"
fi
echo

echo "[3/6] Confirm decommission..."

read -rp "Type 'yes' to decommission $BE: " CONFIRM

if [[ "$CONFIRM" != "yes" ]]; then
    echo "Cancelled"
    exit 0
fi


mysql_cmd "
ALTER SYSTEM DECOMMISSION BACKEND '$BE';
"

echo "Decommission started."
echo

echo "[4/6] Waiting for FE metadata removal..."

while true; do

    BACKENDS="$(get_backends)" ||
        die "Cannot query backend status"

    TARGET_ROW="$(
        awk -F '\t' \
            -v ip="$BE_HOST" \
            -v port="$BE_HEARTBEAT_PORT" \
            '$2 == ip && $3 == port {print}' \
            <<< "$BACKENDS"
    )"

    if [[ -z "$TARGET_ROW" ]]; then
        echo
        echo "Backend removed from FE metadata."
        break
    fi


    TABLETS="$(awk -F '\t' '{print $12}' <<< "$TARGET_ROW")"
    echo "$(date '+%F %T') TabletNum=$TABLETS"

    sleep "$POLL_INTERVAL"
done


echo
echo "[5/6] Verifying remaining backend health..."

BACKENDS="$(get_backends)"

HEALTHY_REMAINING="$(
    awk -F '\t' '
        $9=="true" && $10!="true" && $11!="true" {
            count++
        }
        END {print count+0}
    ' <<< "$BACKENDS"
)"

echo "Healthy active backends: $HEALTHY_REMAINING"

if (( HEALTHY_REMAINING < MAX_REPLICATION_FACTOR )); then
    die "Replica safety check failed after decommission"
fi

echo
echo "[6/6] Remote BE maintenance action"

read -rp "SSH username for $BE_HOST: " SSH_USER
read -rp "Stop starrocks-be on $BE_HOST now? (yes/no): " STOP

if [[ "$STOP" == "yes" ]]; then

    ssh \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=yes \
        -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" \
        "$SSH_USER@$BE_HOST" \
        "sudo -n systemctl stop starrocks-be" \
        || die "Remote stop failed"

    echo "starrocks-be stopped on $BE_HOST"

else
    echo "Leaving BE service running."
fi

unset MYSQL_PASSWORD

echo
echo "==================================="
echo "Decommission workflow completed."
echo "==================================="