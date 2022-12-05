cd "$(git rev-parse --show-toplevel)"

if (docker network ls | findstr -i confluent)
{
    Write-Output "Docker network 'confluent' already exists"
} else 
{
    Write-Output "Creating 'confluent' docker network..."
    docker network create confluent > $null
    Write-Output "Created!"        
}

function Start-Zookeeper {
    Write-Output "Starting 'zookeeper' docker container..."
    docker run -d `
        --net=confluent `
        --name=zookeeper `
        -e ZOOKEEPER_CLIENT_PORT=2181 `
        confluentinc/cp-zookeeper:5.0.0 > $null
    Write-Output "Started."
}

if (docker ps -a | findstr -i zookeeper)
{
    Write-Output "Docker container 'zookeeper' already exists, stopping it..."
    docker stop zookeeper > $null
    docker rm zookeeper > $null
    Write-Output "Stopped."
}

Start-Zookeeper

function Start-Kafka {
    Write-Output "Starting 'kafka' docker container..."
    docker run -d `
        --net=confluent `
        --name=kafka `
        -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 `
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 `
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
        confluentinc/cp-kafka:5.0.0 > $null
    Write-Output "Started!"
}

if (docker ps -a | findstr -i kafka)
{
    Write-Output "Docker container 'kafka' already exists, stopping it..."
    docker stop kafka > $null
    docker rm kafka > $null
    Write-Output "Stopped!"
}

Start-Kafka

function Start-Schema-Registry {
    Write-Output "Starting 'schema-registry' docker container..."
    docker run -d `
        --net=confluent `
        --name=schema-registry `
        -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:2181 `
        -e SCHEMA_REGISTRY_HOST_NAME=schema-registry `
        -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 `
        confluentinc/cp-schema-registry:5.0.0 > $null
    Write-Output "Started!"
}

if (docker ps -a | findstr -i schema-registry)
{
    Write-Output "Docker container 'schema-registry' already exists, stopping it..."
    docker stop schema-registry > $null
    docker rm schema-registry > $null
    Write-Output "Stopped!"
}

Start-Schema-Registry

function Start-Kafka-Rest {
    Write-Output "Starting 'kafka-rest' docker container..."
    docker run -d `
        --net=confluent `
        --name=kafka-rest `
        -e KAFKA_REST_ZOOKEEPER_CONNECT=zookeeper:2181 `
        -e KAFKA_REST_LISTENERS=http://0.0.0.0:8082 `
        -e KAFKA_REST_SCHEMA_REGISTRY_URL=http://schema-registry:8081 `
        -e KAFKA_REST_HOST_NAME=kafka-rest `
        confluentinc/cp-kafka-rest:5.0.0 > $null
    Write-Output "Started!"
}

if (docker ps -a | findstr -i kafka-rest)
{
    Write-Output "Docker container 'kafka-rest' already exists, stopping it..."
    docker stop kafka-rest > $null
    docker rm kafka-rest > $null
    Write-Output "Stopped!"
}

Start-Kafka-Rest

Write-Output "Building project (this may take some time)..."
docker build -t singlestore-kafka-connect . > $null 2> $null


if (docker ps -a | findstr -i singlestore-kafka-connect)
{
    Write-Output "Docker container 'singlestore-kafka-connect' already exists, stopping it..."
    docker stop singlestore-kafka-connect > $null
    docker rm singlestore-kafka-connect > $null
    Write-Output "Stopped!"
}
Write-Output "Success!"

Write-Output "Copying 'singlestore-kafka-connector'..."
docker run `
    -d `
    --rm `
    --net=confluent `
    --name singlestore-kafka-connect `
    singlestore-kafka-connect `
    tail -f /dev/null > $null
docker cp singlestore-kafka-connect:/home/app/target/singlestore-kafka-connector-1.1.2.jar "$env:TEMP"
docker stop singlestore-kafka-connect > $null
Write-Output "Success!"

function Start-Kafka-Connect {
    Write-Output "Starting 'kafka-connect' docker container..."

    # replace backslashes with slashes, colons with nothing,
    # convert to lower case and trim last /
    $nixTempPath = (("$env:TEMP" -replace "\\","/") -replace ":","").ToLower().Trim("/")
    $kafkaConnectorVolumes = $nixTempPath + "/singlestore-kafka-connector-1.1.2.jar:/usr/share/java/singlestore-kafka-connector-1.1.2.jar"

    docker run -d `
        --name=kafka-connect `
        --net=confluent `
        -e CONNECT_PRODUCER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor `
        -e CONNECT_CONSUMER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor `
        -e CONNECT_BOOTSTRAP_SERVERS=kafka:9092 `
        -e CONNECT_REST_PORT=8082 `
        -e CONNECT_GROUP_ID="quickstart" `
        -e CONNECT_CONFIG_STORAGE_TOPIC="quickstart-config" `
        -e CONNECT_OFFSET_STORAGE_TOPIC="quickstart-offsets" `
        -e CONNECT_STATUS_STORAGE_TOPIC="quickstart-status" `
        -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 `
        -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 `
        -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 `
        -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" `
        -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false `
        -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" `
        -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" `
        -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" `
        -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect" `
        -e CONNECT_LOG4J_ROOT_LOGLEVEL=DEBUG `
        -e CONNECT_PLUGIN_PATH=/usr/share/java `
        -e CONNECT_REST_HOST_NAME="kafka-connect" `
        -v /tmp/quickstart/file:/tmp/quickstart `
        -v $kafkaConnectorVolumes `
        confluentinc/cp-kafka-connect:5.0.0 > $null
    Write-Output "Started!"
} 

if (docker ps -a | findstr kafka-connect) 
{
    Write-Output "Docker container 'kafka-connect' already exists, stopping it..."
    docker stop kafka-connect > $null
    docker rm kafka-connect > $null
    Write-Output "Stopped!"
}

Start-Kafka-Connect

Write-Output "Copying 'SingleStore JDBC driver'..."
docker exec kafka-connect curl https://repo.maven.apache.org/maven2/com/singlestore/singlestore-jdbc-client/1.1.0/singlestore-jdbc-client-1.1.0.jar --output /usr/share/java/kafka/singlestore-jdbc-client-1.1.0.jar > $null 2> $null
Write-Output "Success!"

function Start-SingleStore {
    Write-Output "Starting 'singlestore-kafka' docker container..."
    
    docker run -i --init `
        --name singlestore-kafka `
        -e LICENSE_KEY="$env:LICENSE_KEY" `
        -e ROOT_PASSWORD=root `
        -p 3306:3306 `
        --net=confluent `
        memsql/cluster-in-a-box > $null

    if ($LastExitCode -ne 0)
    {
        Write-Output "Failed to start 'singlestore-kafka' container"
        Write-Output "Aborting..."
        exit 1
    }

    docker start singlestore-kafka > $null
    Write-Output "Success!"
}

if (docker ps -a | findstr singlestore-kafka) 
{
    Write-Output "Docker container 'singlestore-kafka' already exists, stopping it..."
    docker stop singlestore-kafka > $null 
    docker rm singlestore-kafka > $null 
    Write-Output "Stopped!"
}

Start-SingleStore

function Wait-SingleStore-Start
{
    Write-Output "Waiting for SingleStore to start..."
    while($true)
    {
        docker exec singlestore-kafka memsql -u root -proot -e "select 1" > $null 2> $null
        if ($LastExitCode -eq 0) 
        {
            break
        }
        Write-Output "."
        Start-Sleep -Seconds 0.2
    }
    Write-Output "Success!"
}

Wait-SingleStore-Start

Write-Output "Creating 'test' SingleStore database..."
docker exec singlestore-kafka memsql -u root -proot -e "create database if not exists test;" > $null 2> $null 
Write-Output "Success!"

function Wait-Kafka-Connect-Start() {
    Write-Output "Waiting for kafka-connect to start..."

    while ($true)
    {
        docker exec kafka-connect curl -s -X GET http://kafka-connect:8082/connectors > $null 2> $null
        if ($LastExitCode -eq 0)
        {
            break
        }
        Write-Output "."
        Start-Sleep -Seconds 0.2
    }
    Write-Output "Success!"
}

Wait-Kafka-Connect-Start

function Start-Kafka-Connect-Job {
    docker exec kafka-connect curl -X POST -H "Content-Type: application/json" `
        --data '{
            \"name\": \"singlestore-sink-connector\", 
            \"config\": { 
                \"connector.class\":\"com.singlestore.kafka.SingleStoreSinkConnector\", 
                \"tasks.max\":\"1\", 
                \"topics\":\"singlestore_json_songs\", 
                \"connection.ddlEndpoint\" : \"singlestore-kafka:3306\", 
                \"connection.database\" : \"test\", 
                \"connection.user\" : \"root\", 
                \"connection.password\" : \"root\" 
            } 
        }' `
        http://kafka-connect:8082/connectors > $null 2> $null
}

Write-Output "Starting 'singlestore-kafka-connect' job..."
Start-Kafka-Connect-Job
while ($true)
{
    $res = docker exec kafka-connect curl -s -X GET http://kafka-connect:8082/connectors/singlestore-sink-connector | findstr 404
    if ($res -eq $null)
    {
        break    
    }
    Write-Output "."
    Start-Sleep -Seconds 0.2
}
Write-Output "Success!"