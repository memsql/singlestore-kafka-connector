cd "$(git rev-parse --show-toplevel)"
Write-Output "Entered top-level of the repo"

for ($i = 1; $i -le 10; $i++)
{
    docker run `
        --net=confluent-short-demo `
        --rm `
        -e CLASSPATH=/usr/share/java/monitoring-interceptors/monitoring-interceptors-5.0.0.jar `
        --mount type=bind,source="$(pwd)"/demo/data/songs.json,target=/opt/songs.json `
        confluentinc/cp-kafka-connect:5.0.0 `
        bash -c 'cat /opt/songs.json | kafka-console-producer --request-required-acks 1 --broker-list kafka-short-demo:9092 --topic singlestore_json_songs --producer-property interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor --producer-property acks=1' > $null 2> $null
    Write-Output "Data produced. $i of 10"
}
