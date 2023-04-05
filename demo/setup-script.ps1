cd "$(git rev-parse --show-toplevel)"

if (docker network ls | findstr -i confluent-short-demo)
{
    Write-Output "Docker network 'confluent-short-demo' already exists"
} else 
{
    Write-Output "Creating 'confluent-short-demo' docker network..."
    docker network create confluent-short-demo > $null
    Write-Output "Created!"        
}

function Start-Zookeeper {
    Write-Output "Starting 'zookeeper-short-demo' docker container..."
    docker run -d `
        --net=confluent-short-demo `
        --name=zookeeper-short-demo `
        -e ZOOKEEPER_CLIENT_PORT=2181 `
        confluentinc/cp-zookeeper:5.0.0 > $null
    Write-Output "Started."
}

if (docker ps -a | findstr -i zookeeper-short-demo)
{
    Write-Output "Docker container 'zookeeper-short-demo' already exists, stopping it..."
    docker stop zookeeper-short-demo > $null
    docker rm zookeeper-short-demo > $null
    Write-Output "Stopped."
}

Start-Zookeeper

function Start-Kafka {
    Write-Output "Starting 'kafka-short-demo' docker container..."
    docker run -d `
        --net=confluent-short-demo `
        --name=kafka-short-demo `
        -e KAFKA_ZOOKEEPER_CONNECT=zookeeper-short-demo:2181 `
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-short-demo:9092 `
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
        confluentinc/cp-kafka:5.0.0 > $null
    Write-Output "Started!"
}

if (docker ps -a | findstr -i kafka-short-demo)
{
    Write-Output "Docker container 'kafka-short-demo' already exists, stopping it..."
    docker stop kafka-short-demo > $null
    docker rm kafka-short-demo > $null
    Write-Output "Stopped!"
}

Start-Kafka

function Start-Schema-Registry {
    Write-Output "Starting 'schema-registry-short-demo' docker container..."
    docker run -d `
        --net=confluent-short-demo `
        --name=schema-registry-short-demo `
        -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper-short-demo:2181 `
        -e SCHEMA_REGISTRY_HOST_NAME=schema-registry-short-demo `
        -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 `
        confluentinc/cp-schema-registry:5.0.0 > $null
    Write-Output "Started!"
}

if (docker ps -a | findstr -i schema-registry-short-demo)
{
    Write-Output "Docker container 'schema-registry-short-demo' already exists, stopping it..."
    docker stop schema-registry-short-demo > $null
    docker rm schema-registry-short-demo > $null
    Write-Output "Stopped!"
}

Start-Schema-Registry

function Start-Kafka-Rest {
    Write-Output "Starting 'kafka-rest-short-demo' docker container..."
    docker run -d `
        --net=confluent-short-demo `
        --name=kafka-rest-short-demo `
        -e KAFKA_REST_ZOOKEEPER_CONNECT=zookeeper-short-demo:2181 `
        -e KAFKA_REST_LISTENERS=http://0.0.0.0:8082 `
        -e KAFKA_REST_SCHEMA_REGISTRY_URL=http://schema-registry-short-demo:8081 `
        -e KAFKA_REST_HOST_NAME=kafka-rest-short-demo `
        confluentinc/cp-kafka-rest:5.0.0 > $null
    Write-Output "Started!"
}

if (docker ps -a | findstr -i kafka-rest-short-demo)
{
    Write-Output "Docker container 'kafka-rest-short-demo' already exists, stopping it..."
    docker stop kafka-rest-short-demo > $null
    docker rm kafka-rest-short-demo > $null
    Write-Output "Stopped!"
}

Start-Kafka-Rest

Write-Output "Building project (this may take some time)..."
docker build -t singlestore-kafka-connect-short-demo . > $null 2> $null


if (docker ps -a | findstr -i singlestore-kafka-connect-short-demo)
{
    Write-Output "Docker container 'singlestore-kafka-connect-short-demo' already exists, stopping it..."
    docker stop singlestore-kafka-connect-short-demo > $null
    docker rm singlestore-kafka-connect-short-demo > $null
    Write-Output "Stopped!"
}
Write-Output "Success!"

Write-Output "Copying 'singlestore-kafka-connector'..."
docker run `
    -d `
    --rm `
    --net=confluent-short-demo `
    --name singlestore-kafka-connect-short-demo `
    singlestore-kafka-connect-short-demo `
    tail -f /dev/null > $null
docker cp singlestore-kafka-connect-short-demo:/home/app/target/singlestore-kafka-connector-1.2.0-beta.jar "$env:TEMP"
docker stop singlestore-kafka-connect-short-demo > $null
Write-Output "Success!"

function Start-Kafka-Connect {
    Write-Output "Starting 'kafka-connect-short-demo' docker container..."

    # replace backslashes with slashes, colons with nothing,
    # convert to lower case and trim last /
    $nixTempPath = (("$env:TEMP" -replace "\\","/") -replace ":","").ToLower().Trim("/")
    $kafkaConnectorVolumes = $nixTempPath + "/singlestore-kafka-connector-1.2.0-beta.jar:/usr/share/java/singlestore-kafka-connector-1.2.0-beta.jar"

    docker run -d `
        --name=kafka-connect-short-demo `
        --net=confluent-short-demo `
        -e CONNECT_PRODUCER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor `
        -e CONNECT_CONSUMER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor `
        -e CONNECT_BOOTSTRAP_SERVERS=kafka-short-demo:9092 `
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
        -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect-short-demo" `
        -e CONNECT_LOG4J_ROOT_LOGLEVEL=DEBUG `
        -e CONNECT_PLUGIN_PATH=/usr/share/java `
        -e CONNECT_REST_HOST_NAME="kafka-connect-short-demo" `
        -v /tmp/quickstart/file:/tmp/quickstart `
        -v $kafkaConnectorVolumes `
        confluentinc/cp-kafka-connect:5.0.0 > $null
    Write-Output "Started!"
} 

if (docker ps -a | findstr kafka-connect-short-demo)
{
    Write-Output "Docker container 'kafka-connect-short-demo' already exists, stopping it..."
    docker stop kafka-connect-short-demo > $null
    docker rm kafka-connect-short-demo > $null
    Write-Output "Stopped!"
}

Start-Kafka-Connect

Write-Output "Copying 'SingleStore JDBC driver'..."
docker exec kafka-connect-short-demo curl https://repo.maven.apache.org/maven2/com/singlestore/singlestore-jdbc-client/1.1.0/singlestore-jdbc-client-1.1.0.jar --output /usr/share/java/kafka/singlestore-jdbc-client-1.1.0.jar > $null 2> $null
Write-Output "Success!"

function Start-SingleStore {
    Write-Output "Starting 'singlestore-kafka-short-demo' docker container..."
    
    docker run -i --init `
        --name singlestore-kafka-short-demo `
        -e LICENSE_KEY="$env:LICENSE_KEY" `
        -e ROOT_PASSWORD=root `
        -p 3306:3306 `
        --net=confluent-short-demo `
        memsql/cluster-in-a-box > $null

    if ($LastExitCode -ne 0)
    {
        Write-Output "Failed to start 'singlestore-kafka-short-demo' container"
        Write-Output "Aborting..."
        exit 1
    }

    docker start singlestore-kafka-short-demo > $null
    Write-Output "Success!"
}

if (docker ps -a | findstr singlestore-kafka-short-demo)
{
    Write-Output "Docker container 'singlestore-kafka-short-demo' already exists, stopping it..."
    docker stop singlestore-kafka-short-demo > $null
    docker rm singlestore-kafka-short-demo > $null
    Write-Output "Stopped!"
}

Start-SingleStore

function Wait-SingleStore-Start
{
    Write-Output "Waiting for SingleStore to start..."
    while($true)
    {
        docker exec singlestore-kafka-short-demo memsql -u root -proot -e "select 1" > $null 2> $null
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
docker exec singlestore-kafka-short-demo memsql -u root -proot -e "create database if not exists test;" > $null 2> $null
Write-Output "Success!"

function Wait-Kafka-Connect-Start() {
    Write-Output "Waiting for kafka-connect-short-demo to start..."

    while ($true)
    {
        docker exec kafka-connect-short-demo curl -s -X GET http://kafka-connect-short-demo:8082/connectors > $null 2> $null
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
    docker exec kafka-connect-short-demo curl -X POST -H "Content-Type: application/json" `
        --data '{
            \"name\": \"singlestore-sink-connector\", 
            \"config\": { 
                \"connector.class\":\"com.singlestore.kafka.SingleStoreSinkConnector\", 
                \"tasks.max\":\"1\", 
                \"topics\":\"singlestore_json_songs\", 
                \"connection.ddlEndpoint\" : \"singlestore-kafka-short-demo:3306\",
                \"connection.database\" : \"test\", 
                \"connection.user\" : \"root\", 
                \"connection.password\" : \"root\" 
            } 
        }' `
        http://kafka-connect-short-demo:8082/connectors > $null 2> $null
}

Write-Output "Starting 'singlestore-kafka-connect-short-demo' job..."
Start-Kafka-Connect-Job
while ($true)
{
    $res = docker exec kafka-connect-short-demo curl -s -X GET http://kafka-connect-short-demo:8082/connectors/singlestore-sink-connector | findstr 404
    if ($res -eq $null)
    {
        break    
    }
    Write-Output "."
    Start-Sleep -Seconds 0.2
}
Write-Output "Success!"
