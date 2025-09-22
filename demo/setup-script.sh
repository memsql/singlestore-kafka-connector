#!/usr/bin/env bash
if ! cd "$(git rev-parse --show-toplevel)" ; then
    echo "Failed to enter top-level of the repo"
    echo "Aborting..."
    exit 1
fi
echo "Entered top-level of the repo"

if docker network ls | grep confluent-short-demo >/dev/null ;
  then echo "Docker network 'confluent-short-demo' already exists";
  else
    echo -n "Creating 'confluent-short-demo' docker network..."
    docker network create confluent-short-demo
    echo ". Created!"
fi

zookeeper-start() {
  echo -n "Starting 'zookeeper-short-demo' docker container..."
  docker run -d \
    --net=confluent-short-demo \
    --name=zookeeper-short-demo \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:5.0.0 >/dev/null
    echo ". Started!"
}

if docker ps -a | grep zookeeper-short-demo ;
  then
    echo -n "Docker container 'zookeeper-short-demo' already exists, stopping it..."
    docker stop zookeeper-short-demo >/dev/null
    docker rm zookeeper-short-demo >/dev/null
    echo ". Stopped!"
fi

zookeeper-start

kafka-start() {
  echo -n "Starting 'kafka-short-demo' docker container..."
  docker run -d \
    --net=confluent-short-demo \
    --name=kafka-short-demo \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper-short-demo:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-short-demo:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:5.0.0 >/dev/null
    echo ". Started!"
}

if docker ps -a | grep kafka-short-demo ;
  then
    echo -n "Docker container 'kafka-short-demo' already exists, stopping it..."
    docker stop kafka-short-demo >/dev/null
    docker rm kafka-short-demo   >/dev/null
    echo ". Stopped!"
fi

kafka-start

schema-registry-start() {
  echo -n "Starting 'schema-registry-short-demo' docker container..."
  docker run -d \
    --net=confluent-short-demo \
    --name=schema-registry-short-demo \
    -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper-short-demo:2181 \
    -e SCHEMA_REGISTRY_HOST_NAME=schema-registry-short-demo \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
    confluentinc/cp-schema-registry:5.0.0 >/dev/null
    echo ". Started!"
}

if docker ps -a | grep schema-registry-short-demo ;
  then
    echo -n "Docker container 'schema-registry-short-demo' already exists, stopping it..."
    docker stop schema-registry-short-demo >/dev/null
    docker rm schema-registry-short-demo >/dev/null
    echo ". Stopped!"
fi

schema-registry-start

kafka-rest-start() {
  echo -n "Starting 'kafka-rest-short-demo' docker container..."
  docker run -d \
    --net=confluent-short-demo \
    --name=kafka-rest-short-demo \
    -e KAFKA_REST_ZOOKEEPER_CONNECT=zookeeper-short-demo:2181 \
    -e KAFKA_REST_LISTENERS=http://0.0.0.0:8082 \
    -e KAFKA_REST_SCHEMA_REGISTRY_URL=http://schema-registry-short-demo:8081 \
    -e KAFKA_REST_HOST_NAME=kafka-rest-short-demo \
    confluentinc/cp-kafka-rest:5.0.0 >/dev/null
    echo ". Started!"
}

if docker ps -a | grep kafka-rest-short-demo ;
  then
    echo -n "Docker container 'kafka-rest-short-demo' already exists, stopping it..."
    docker stop kafka-rest-short-demo >/dev/null
    docker rm kafka-rest-short-demo >/dev/null
    echo ". Stopped!"
fi

kafka-rest-start

echo -n "Building project (this may take some time)..."
docker build -t singlestore-kafka-connect-short-demo . >/dev/null 2>/dev/null

if docker ps -a | grep singlestore-kafka-connect-short-demo ;
  then
    echo -n "Docker container 'singlestore-kafka-connect-short-demo' already exists, stopping it..."
    docker stop singlestore-kafka-connect-short-demo >/dev/null
    docker rm singlestore-kafka-connect-short-demo >/dev/null
    echo ". Stopped!"
fi
echo ". Success!"

echo -n "Copying 'singlestore-kafka-connector'..."
docker run \
    -d \
    --rm \
    --net=confluent-short-demo \
    --name singlestore-kafka-connect-short-demo \
    -v /tmp/quickstart/connect:/tmp/quickstart/connect \
    singlestore-kafka-connect-short-demo \
    tail -f /dev/null >/dev/null 2>/dev/null
docker exec singlestore-kafka-connect-short-demo cp /home/app/target/singlestore-kafka-connector-1.2.3.jar /tmp/quickstart/connect
docker stop singlestore-kafka-connect-short-demo >/dev/null 2>/dev/null
echo ". Success!"

kafka-connect-start() {
  echo -n "Starting 'kafka-connect-short-demo' docker container..."
  docker run -d \
    --name=kafka-connect-short-demo \
    --net=confluent-short-demo \
    -e CONNECT_PRODUCER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor \
    -e CONNECT_CONSUMER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor \
    -e CONNECT_BOOTSTRAP_SERVERS=kafka-short-demo:9092 \
    -e CONNECT_REST_PORT=8082 \
    -e CONNECT_GROUP_ID="quickstart" \
    -e CONNECT_CONFIG_STORAGE_TOPIC="quickstart-config" \
    -e CONNECT_OFFSET_STORAGE_TOPIC="quickstart-offsets" \
    -e CONNECT_STATUS_STORAGE_TOPIC="quickstart-status" \
    -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
    -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false \
    -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
    -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect-short-demo" \
    -e CONNECT_LOG4J_ROOT_LOGLEVEL=DEBUG \
    -e CONNECT_PLUGIN_PATH=/usr/share/java \
    -e CONNECT_REST_HOST_NAME="kafka-connect-short-demo" \
    -v /tmp/quickstart/file:/tmp/quickstart \
    -v /tmp/quickstart/connect/singlestore-kafka-connector-1.2.3.jar:/usr/share/java/singlestore-kafka-connector-1.2.3.jar \
    confluentinc/cp-kafka-connect:5.0.0 >/dev/null
    echo ". Started!"
}

if docker ps -a | grep kafka-connect-short-demo ;
  then
    echo -n "Docker container 'kafka-connect-short-demo' already exists, stopping it..."
    docker stop kafka-connect-short-demo >/dev/null
    docker rm kafka-connect-short-demo >/dev/null
    echo ". Stopped!"
fi

kafka-connect-start

echo -n "Copying 'SingleStore JDBC driver'..."
docker exec kafka-connect-short-demo curl https://repo.maven.apache.org/maven2/com/singlestore/singlestore-jdbc-client/1.1.0/singlestore-jdbc-client-1.1.0.jar --output /usr/share/java/kafka/singlestore-jdbc-client-1.1.0.jar >/dev/null 2>/dev/null
echo ". Success!"

singlestore-start() {
  echo -n "Starting 'singlestore-kafka-short-demo' docker container..."
  docker run -i --init \
    --name singlestore-kafka-short-demo \
    -e LICENSE_KEY="$LICENSE_KEY" \
    -e ROOT_PASSWORD=root \
    -p 3306:3306 \
    --net=confluent-short-demo \
    memsql/cluster-in-a-box >/dev/null
  if [[ $? -ne 0 ]] ; then
    echo "Failed to start 'singlestore-kafka-short-demo' container"
    echo "Aborting..."
    exit 1
  fi

  docker start singlestore-kafka-short-demo >/dev/null
  echo ". Success!"
}

if docker ps -a | grep singlestore-kafka-short-demo ;
  then
    echo -n "Docker container 'singlestore-kafka-short-demo' already exists, stopping it..."
    docker stop singlestore-kafka-short-demo >/dev/null
    docker rm singlestore-kafka-short-demo >/dev/null
    echo ". Stopped!"
fi

singlestore-start

singlestore-wait-start() {
  echo -n "Waiting for SingleStore to start..."
  while true; do
      if docker exec singlestore-kafka-short-demo memsql -u root -proot -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

singlestore-wait-start

echo -n "Creating 'test' SingleStore database..."
docker exec singlestore-kafka-short-demo memsql -u root -proot -e "create database if not exists test;"
echo ". Success!"

kafka-connect-wait-start() {
  echo -n "Waiting for kafka-connect-short-demo to start..."

  while true; do
      docker exec kafka-connect-short-demo curl -s -X GET http://kafka-connect-short-demo:8082/connectors >/dev/null 2>/dev/null
      if [[ $? -eq 0 ]] ; then
        break;
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

kafka-connect-wait-start

kafka-connect-job-start() {
  docker exec kafka-connect-short-demo curl -X POST -H "Content-Type: application/json" \
    --data '{
          "name": "singlestore-sink-connector",
          "config": {
                  "connector.class":"com.singlestore.kafka.SingleStoreSinkConnector",
                  "tasks.max":"1",
                  "topics":"singlestore_json_songs",
                  "connection.ddlEndpoint" : "singlestore-kafka-short-demo:3306",
                  "connection.database" : "test",
                  "connection.user" : "root",
                  "connection.password" : "root"
          }
    }' \
    http://kafka-connect-short-demo:8082/connectors >/dev/null 2>/dev/null
}

echo -n "Starting 'singlestore-kafka-connect-short-demo' job..."
while true; do
      kafka-connect-job-start
      if [[ $(docker exec kafka-connect-short-demo curl -s -X GET http://kafka-connect-short-demo:8082/connectors/singlestore-sink-connector | grep -c 404) -eq 0 ]] ; then
        break;
      fi
      echo -n "."
      sleep 0.2
done
echo ". Success!"
