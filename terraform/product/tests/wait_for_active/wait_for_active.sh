#!/usr/bin/env sh
# Poll `juju status` until an application reaches active status or a timeout elapses.
#
# Invoked by Terraform's external data source (tests/wait_for_active/main.tf) with positional
# arguments: model_uuid, app_name, timeout_seconds. Prints {"status":"active"} to stdout on
# success. On timeout, prints the last known status plus the full `juju status` JSON to stderr
# and exits non-zero so the external data source surfaces a Terraform test failure.
set -eu

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <model_uuid> <app_name> <timeout_seconds>" >&2
    exit 1
fi

model_uuid="$1"
app_name="$2"
timeout_seconds="$3"

poll_interval_seconds="${WAIT_FOR_ACTIVE_POLL_INTERVAL_SECONDS:-10}"

deadline=$(($(date +%s) + timeout_seconds))

status_json="{}"
current="unknown"

while [ "$(date +%s)" -lt "$deadline" ]; do
    status_json=$(juju status --model "$model_uuid" --format json)
    current=$(printf '%s' "$status_json" | jq -r --arg app "$app_name" '.applications[$app]["application-status"].current // "unknown"')
    if [ "$current" = "active" ] || [ "$current" = "error" ]; then
        break
    fi
    sleep "$poll_interval_seconds"
done

if [ "$current" != "active" ]; then
    echo "$app_name did not reach active status (last seen: $current)" >&2
    printf '%s\n' "$status_json" >&2
    exit 1
fi

printf '{"status":"active"}\n'
