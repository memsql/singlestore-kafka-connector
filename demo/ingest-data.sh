#!/usr/bin/env bash

if ! cd "$(git rev-parse --show-toplevel)" ; then
    echo "Failed to enter top-level of the repo"
    echo "Aborting..."
    exit 1
fi
echo "Entered top-level of the repo"

for i in {1..10}
do
  docker run \
    --net=confluent \
    --rm \
    -e CLASSPATH=/usr/share/java/monitoring-interceptors/monitoring-interceptors-5.0.0.jar \
    --mount type=bind,source="$(pwd)"/demo/data/songs.json,target=/opt/songs.json \
    confluentinc/cp-kafka-connect:5.0.0 \
    bash -c 'cat /opt/songs.json | kafka-console-producer --request-required-acks 1 --broker-list kafka:9092 --topic singlestore_json_songs --producer-property interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor --producer-property acks=1'
    echo "Data produced. $i of 10"
done