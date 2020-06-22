#!/usr/bin/env bash

docker run \
    --net=confluent \
    --rm \
    -e CLASSPATH=/usr/share/java/monitoring-interceptors/monitoring-interceptors-5.0.0.jar \
    --mount type=bind,source="$(pwd)"/data/songs.json,target=/opt/songs.json \
    confluentinc/cp-kafka-connect:5.0.0 \
    bash -c 'cat /opt/songs.json | kafka-console-producer --request-required-acks 1 --broker-list kafka:9092 --topic memsql-json-songs --producer-property interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor --producer-property acks=1' >/dev/null
echo "Produced 'songs.json' data."