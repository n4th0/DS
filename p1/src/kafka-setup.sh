#!/bin/bash
# Creates the four topics required by the Water Management system.
# Called automatically by the kafka-init container in docker-compose.
# Can also be run manually:  KAFKA_BROKER=localhost:19092 ./kafka-setup.sh

set -e

KAFKA_BROKER="${KAFKA_BROKER:-localhost:19092}"

echo "==> Waiting for Kafka at ${KAFKA_BROKER} ..."
until kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" --list >/dev/null 2>&1; do
  echo "   ... not ready yet, retrying in 2s"
  sleep 2
done
echo "==> Kafka is up."

TOPICS=(
  "wm_requests"      # FO  -> Central  (activation requests)
  "wm_commands"      # Central -> WS_E (START / STOP / BLOCK / ACTIVATE)
  "wm_status"        # WS_E -> Central (flow rate + volume updates)
  "wm_notifications" # Central -> FO   (auth, denials, summaries)
)

for t in "${TOPICS[@]}"; do
  if kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" --list | grep -qx "${t}"; then
    echo "   [skip] topic '${t}' already exists"
  else
    kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" \
      --create --topic "${t}" --partitions 3 --replication-factor 1
    echo "   [ok]   created topic '${t}'"
  fi
done

echo "==> All topics ready:"
kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" --list
