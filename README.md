# MemSQL Kafka Connector
## Version: 0.0.1 [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Getting Started

## Configuration

The `memsql-kafka-connector` is configurable via json file that should be
specified before starting kafka-connect job.

| Option                               | Description
| -                                    | -
| `connection.ddlEndpoint`  (required) | Hostname or IP address of the MemSQL Master Aggregator in the format `host[:port]` (port is optional). Ex. `master-agg.foo.internal:3308` or `master-agg.foo.internal`
| `connection.dmlEndpoints`            | Hostname or IP address of MemSQL Aggregator nodes to run queries against in the format `host[:port],host[:port],...` (port is optional, multiple hosts separated by comma). Ex. `child-agg:3308,child-agg2` (default: `ddlEndpoint`)
| `connection.database`     (required) | If set, all connections will default to using this database (default: empty)
| `connection.user`                    | MemSQL username (default: `root`)
| `connection.password`                | MemSQL password (default: no password)
| `params.<name>`                      | Specify a specific MySQL or JDBC parameter which will be injected into the connection URI (default: empty)
| `max.retries`                        | The maximum number of times to retry on errors before failing the task. (default: 10)
| `retry.backoff.ms`                   | The time in milliseconds to wait following an error before a retry attempt is made. (default 3000)
| `memsql.loadDataCompression`         | Compress data on load; one of (`GZip`, `LZ4`, `Skip`) (default: GZip)

### Config example
```
{
    "name": "memsql-sink-connector",
    "config": {
        "connector.class":"com.memsql.kafka.MemSQLSinkConnector",
        "tasks.max":"1",
        "topics":"topic-test-1,topic-test-2",
        "connection.ddlEndpoint" : "memsql-host1:3306",
        "connection.ddlEndpoint" : "memsql-host2:3306,memsql-host3:3306",
        "connection.database" : "test",
        "connection.user" : "root",
        "params.connectTimeout" : "10000"
        "params.ssl" : "true"
    }
}
```