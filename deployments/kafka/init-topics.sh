#!/bin/bash
set -e

BROKERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

echo "Waiting for all 3 Kafka brokers to be ready..."
while :; do
  ready=$(/opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BROKERS" 2>/dev/null \
    | grep -cE '^kafka-[1-3]:9092' || true)
  if [ "$ready" -ge 3 ]; then
    break
  fi
  echo "  ready brokers: $ready/3"
  sleep 2
done
echo "All 3 brokers are ready."

echo "Creating profile-updates topic (RF=3, min.insync.replicas=2)..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKERS" --create \
  --topic profile-updates \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000 \
  --if-not-exists

echo "Creating profile-updates-dlq topic (RF=3, min.insync.replicas=2)..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKERS" --create \
  --topic profile-updates-dlq \
  --partitions 1 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --if-not-exists

echo "Topics created successfully"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKERS" --list
