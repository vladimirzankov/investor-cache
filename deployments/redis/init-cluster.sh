#!/bin/sh
set -e

for port in 7001 7002 7003 7004 7005 7006; do
  host="redis-node-$((port - 7000))"
  echo "Waiting for $host:$port ..."
  until redis-cli -h "$host" -p "$port" ping 2>/dev/null | grep -q PONG; do
    sleep 1
  done
  echo "$host:$port is ready"
done

echo "Creating Redis Cluster..."
redis-cli --cluster create \
  redis-node-1:7001 redis-node-2:7002 redis-node-3:7003 \
  redis-node-4:7004 redis-node-5:7005 redis-node-6:7006 \
  --cluster-replicas 1 --cluster-yes

echo "Redis Cluster created successfully"
