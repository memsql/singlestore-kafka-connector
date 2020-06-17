#!/usr/bin/env bash
if ! cd "$(git rev-parse --show-toplevel)" ; then
    echo "Failed to enter top-level of the repo"
    echo "Aborting"
    exit 1
fi
echo "Entered top-level of the repo"

if docker network ls | grep confluent >/dev/null ;
  then echo "Docker network 'confluent' already exists";
fi

echo "Creating 'confluent' docker network..."
docker network create confluent
echo "Docker network 'confluent' created"

zookeeper-start() {
  echo "Starting 'zookeeper' docker container..."
  docker run -d \
    --net=confluent \
    --name=zookeeper \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:5.0.0 >/dev/null
    echo "Docker container 'zookeeper' successfully started"
}

if docker ps -a | grep zookeeper ;
  then
    echo "Docker container 'zookeeper' already exists, stopping it..."
    docker stop zookeeper >/dev/null
    docker rm zookeeper >/dev/null
    echo "Docker container 'zookeeper' successfully stopped"
fi

zookeeper-start

kafka-start() {
  echo "Starting 'kafka' docker container..."
  docker run -d \
    --net=confluent \
    --name=kafka \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:5.0.0 >/dev/null
    echo "Docker container 'kafka' successfully started"
}

if docker ps -a | grep kafka ;
  then
    echo "Docker container 'kafka' already exists, stopping it..."
    docker stop kafka >/dev/null
    docker rm kafka >/dev/null
    echo "Docker container 'kafka' successfully stopped"
fi

kafka-start

schema-registry-start() {
  echo "Starting 'schema-registry' docker container..."
  docker run -d \
    --net=confluent \
    --name=schema-registry \
    -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:2181 \
    -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
    confluentinc/cp-schema-registry:5.0.0 >/dev/null
    echo "Docker container 'schema-registry' successfully started"
}

if docker ps -a | grep schema-registry ;
  then
    echo "Docker container 'schema-registry' already exists, stopping it..."
    docker stop schema-registry >/dev/null
    docker rm schema-registry >/dev/null
    echo "Docker container 'schema-registry' successfully stopped"
fi

schema-registry-start

kafka-rest-start() {
  echo "Starting 'kafka-rest' docker container..."
  docker run -d \
    --net=confluent \
    --name=kafka-rest \
    -e KAFKA_REST_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_REST_LISTENERS=http://0.0.0.0:8082 \
    -e KAFKA_REST_SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    -e KAFKA_REST_HOST_NAME=kafka-rest \
    confluentinc/cp-kafka-rest:5.0.0 >/dev/null
    echo "Docker container 'kafka-rest' successfully started"
}

if docker ps -a | grep kafka-rest ;
  then
    echo "Docker container 'kafka-rest' already exists, stopping it..."
    docker stop kafka-rest >/dev/null
    docker rm kafka-rest >/dev/null
    echo "Docker container 'kafka-rest' successfully stopped."
fi

kafka-rest-start

kafka-wait-start() {
  echo -n "Waiting for kafka to start..."
  while [[ $(docker logs kafka >/dev/null 2>/dev/null | grep 'started' | wc -l) -ne 1 ]]; do
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

kafka-wait-start

create-topic() {
  echo "Creating kafka topic '$1'"
  docker run \
    --net=confluent \
    --rm \
    confluentinc/cp-kafka:5.0.0 \
    kafka-topics --create --topic "$1" --partitions 1 \
    --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
    echo "Kafka topic '$1' created"
}

create-topic quickstart-data
create-topic quickstart-offsets

kafka-connect-start() {
  echo "Starting 'kafka-connect' docker container..."
  docker run -d \
    --name=kafka-connect \
    --net=confluent \
    -e CONNECT_PRODUCER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor \
    -e CONNECT_CONSUMER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor \
    -e CONNECT_BOOTSTRAP_SERVERS=kafka:9092 \
    -e CONNECT_REST_PORT=8082 \
    -e CONNECT_GROUP_ID="quickstart" \
    -e CONNECT_CONFIG_STORAGE_TOPIC="quickstart-config" \
    -e CONNECT_OFFSET_STORAGE_TOPIC="quickstart-offsets" \
    -e CONNECT_STATUS_STORAGE_TOPIC="quickstart-status" \
    -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect" \
    -e CONNECT_LOG4J_ROOT_LOGLEVEL=DEBUG \
    -e CONNECT_PLUGIN_PATH=/usr/share/java \
    -e CONNECT_REST_HOST_NAME="kafka-connect" \
    -v /tmp/quickstart/file:/tmp/quickstart \
    confluentinc/cp-kafka-connect:5.0.0 >/dev/null
    echo "Docker container 'kafka-connect' successfully started"
}

if docker ps -a | grep kafka-connect ;
  then
    echo "Docker container 'kafka-connect' already exists, stopping it..."
    docker stop kafka-connect >/dev/null
    docker rm kafka-connect >/dev/null
    echo "Docker container 'kafka-connect' successfully stopped."
fi

kafka-connect-start

echo "Building project..."
mvn clean package >/dev/null 2>/dev/null
if [[ $? -ne 0 ]] ; then
  echo 'project build failed'; exit 1
fi
echo "Project built"

echo "Copying kafka-connect jar file"
docker cp target/memsql-kafka-connector-1.0-SNAPSHOT-jar-with-dependencies.jar kafka-connect:/usr/share/java/kafka

memsql-start() {
  echo "Starting 'memsql-kafka' docker container..."
  docker run -i --init \
    --name memsql-kafka \
    -e LICENSE_KEY=$LICENSE_KEY \
    -p 3306:3306 \
    --net=confluent \
    memsql/cluster-in-a-box >/dev/null

  docker start memsql-kafka >/dev/null
  echo "Docker container 'memsql-kafka' successfully started"
}

if docker ps -a | grep memsql-kafka ;
  then
    echo "Docker container 'memsql-kafka' already exists, stopping it..."
    docker stop memsql-kafka >/dev/null
    docker rm memsql-kafka >/dev/null
    echo "Docker container 'memsql-kafka' successfully stopped."
fi

memsql-start

memsql-wait-start() {
  echo -n "Waiting for MemSQL to start..."
  while true; do
      if mysql -u root -h 127.0.0.1 -P 3306 -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

memsql-wait-start

kafka-connect-wait-start() {
  echo -n "Waiting for kafka-connect to start..."
  while [[ $(docker logs kafka-connect >/dev/null 2>/dev/null | grep 'Kafka Connect started' | wc -l) -ne 1 ]]; do
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

kafka-connect-wait-start

echo "Starting 'memsql-kafka-connect' job"
docker exec kafka-connect curl -X POST -H "Content-Type: application/json" \
  --data '{
        "name": "memsql-sink-connector",
        "config": {
                "connector.class":"com.memsql.kafka.MemSQLSinkConnector",
                "tasks.max":"1",
                "topics":"quickstart-jdbc-test",
                "connection.ddlEndpoint" : "memsql-kafka:3306",
                "connection.database" : "test",
                "connection.user" : "root",                                                                                                                                                                                        "memsql.loadDataFormat" : "avro"
        }
  }' \
  http://kafka-connect:8082/connectors
echo "'memsql-kafka-connect' job successfully started"