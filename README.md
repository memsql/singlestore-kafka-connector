# SingleStore Kafka Connector
## Version: 1.0.1 [![Continuous Integration](https://circleci.com/gh/memsql/singlestore-kafka-connector/tree/master.svg?style=shield)](https://circleci.com/gh/memsql/memsql-kafka-connector) [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Getting Started

SingleStore Kafka Connector is a [kafka-connector](http://kafka.apache.org/documentation.html#connect)
for loading data from Kafka to SingleStore.

Quickstart guide can be found [here](https://github.com/memsql/singlestore-kafka-connector/blob/master/demo/README.md)

## Configuration

The `singlestore-kafka-connector` is configurable via property file that should be
specified before starting kafka-connect job.

| Option                                         | Description
| -                                              | -
| `connection.ddlEndpoint`  (required)           | Hostname or IP address of the SingleStore Master Aggregator in the format `host[:port]` (port is optional). Ex. `master-agg.foo.internal:3308` or `master-agg.foo.internal`
| `connection.dmlEndpoints`                      | Hostname or IP address of SingleStore Aggregator nodes to run queries against in the format `host[:port],host[:port],...` (port is optional, multiple hosts separated by comma). Ex. `child-agg:3308,child-agg2` (default: `ddlEndpoint`)
| `connection.database`     (required)           | If set, all connections will default to using this database (default: empty)
| `connection.user`                              | SingleStore username (default: `root`)
| `connection.password`                          | SingleStore password (default: no password)
| `params.<name>`                                | Specify a specific MySQL or JDBC parameter which will be injected into the connection URI (default: empty)
| `max.retries`                                  | The maximum number of times to retry on errors before failing the task. (default: 10)
| `fields.whitelist`                             | Specify fields to be inserted to the database. (default: all keys will be used)
| `retry.backoff.ms`                             | The time in milliseconds to wait following an error before a retry attempt is made. (default 3000)
| `tableKey.<index_type>[.name]`                 | Specify additional keys to add to tables created by the connector; value of this property is the comma separated list with names of the columns to apply key; <index_type> one of (`PRIMARY`, `COLUMNSTORE`, `UNIQUE`, `SHARD`, `KEY`);
| `singlestore.loadDataCompression`              | Compress data on load; one of (`GZip`, `LZ4`, `Skip`) (default: GZip)
| `singlestore.metadata.allow`                   | Allows or denies the use of an additional meta-table to save the recording results (default: true)
| `singlestore.metadata.table`                   | Specify the name of the table to save kafka transaction metadata (default: `kafka_connect_transaction_metadata`)
| `singlestore.tableName.<topicName>=<tableName>`| Specify an explicit table name to use for the specified topic

### Config example
```
{
    "name": "singlestore-sink-connector",
    "config": {
        "connector.class":"com.singlestore.kafka.SingleStoreSinkConnector",
        "tasks.max":"1",
        "topics":"topic-test-1,topic-test-2",
        "connection.ddlEndpoint" : "singlestore-host1:3306",
        "connection.dmlEndpoints" : "singlestore-host2:3306,singlestore-host3:3306",
        "connection.database" : "test",
        "connection.user" : "root",
        "params.connectTimeout" : "10000"
        "params.ssl" : "false",
        "tableKey.primary.keyName" : "id",
        "tableKey.key.keyName" : "`col1`, col2, `col3`",
        "singlestore.loadDataCompression" : "LZ4",
        "singlestore.tableName.kafka-topic-example" : "singlestore-table-name"
    }
}
```

## Auto-creation of tables

If the table does not exist, it will be created using the information from the first record.

The table name is the name of the topic. The table schema is taken from record valueSchema.
if valueSchema is not a struct, then a single column with name `data` will be created with the schema of the record.
Table keys are taken from tableKey option.

If the table already exists, all records will be loaded directly into it.
The auto-evolution of the table is not supported yet (all records should have the same schema).

## Exactly once delivery

To achieve exactly once delivery, set `singlestore.metadata.allow` to true.
Then `kafka_connect_transaction_metadata` table will be created.

This table contains an identifier, count of records, and time of each transaction.
The identifier consists of kafka-topic, kafka-partition and kafka-offset. This combination
gives a unique identifier that prevents duplication of data in the SingleStore database.
Kafka saves offsets and increases them only if the kafka-connect job succeeds.
If the job failed, Kafka will restart the job with the same offset. This means that if the data
were written to the database, but the operation failed, Kafka will try to write data with the same
offset and metadata identifier prevent duplication of existing data and simply complete
work successfully.

Data is written to the table and to the `kafka_connect_transaction_metadata` table in one transaction.
Because of this, if some error occurred, no data will be actually added to the database.

To overwrite the name of this table, use `singlestore.metadata.table` option.

## Data Types

`singlestore-kafka-connector` makes such conversions from Kafka types to SingleStore types:

| Kafka Type    | SingleStore Type
| -             | -
| STRUCT        | JSON
| MAP           | JSON
| ARRAY         | JSON
| INT8          | TINYINT
| INT16         | SMALLINT
| INT32         | INT
| INT64         | BIGINT
| FLOAT32       | FLOAT
| FLOAT64       | DOUBLE
| BOOLEAN       | TINYINT
| BYTES         | TEXT
| STRING        | VARBINARY(1024)

## Table keys

To add some column as a key in SingleStore, use `tableKey` parameter like this:

Suppose you have an entity
```
{
    "id" : 123,
    "name" : "Alice"
}
```

and you want to add `id` column as a `PRIMARY KEY` to your SingleStore table. Then add
`"tableKey.primary": "id"` to your configuration. It will create such query during creating a table:
```
    CREATE TABLE IF NOT EXISTS `table` (
        `id` INT NOT NULL,
        `name` TEXT NOT NULL,
        PRIMARY KEY (`id`)
    )
```
You can also specify the name of a key by providing it like this
`"tableKey.primary.someName" : "id"`. It will create a key with a name.
```
    CREATE TABLE IF NOT EXISTS `table` (
        `id` INT NOT NULL,
        `name` TEXT NOT NULL,
        PRIMARY KEY `someName`(`id`)
    )
```

## Table names

By default the `singlestore-kafka-connector` maps data from topics into SingleStore tables by matching the topic name to the table name.
For example, if the kafka topic is called `kafka-example-topic` then the `singlestore-kafka-connector` will load it into the SingleStore 
table called `kafka-example-topic` (the table will be created if it doesn't already exist).

To specify a custom table name, you can use the `singlestore.tableName.<topicName>` parameter.

```
{
    ...
    "singlestore.tableName.foo" : "bar",
    ...
}
```

In this example, data from the kafka topic `foo` will be written to the SingleStore table called `bar`.

You can use this method to specify custom table names for multiple topics:

```
{
    ...
    "singlestore.tableName.kafka-example-topic-1" : "singlestore-table-name-1",
    "singlestore.tableName.kafka-example-topic-2" : "singlestore-table-name-2",
    ...
}
```

## Setting up development environment

 * clone the repository https://github.com/memsql/singlestore-kafka-connector.git
 * open a project with Intellij IDEA
 * to run unit tests use the `unit-tests` run configuration
 * before running integration tests, start [MemSQL CIAB](https://hub.docker.com/r/memsql/cluster-in-a-box) cluster using one of the following run configurations:
   - `ensure-test-singlestore-cluster`
   - `ensure-test-singlestore-cluster-6-7`
   - `ensure-test-singlestore-cluster-6-8`
 * to run integration tests use the `integration-tests` run configuration
