# MemSQL Kafka Connector
## Version: 1.0.0 [![Continuous Integration](https://circleci.com/gh/memsql/memsql-kafka-connector/tree/master.svg?style=shield)](https://circleci.com/gh/memsql/memsql-kafka-connector) [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Getting Started

MemSQL Kafka Connector is a [kafka-connector](http://kafka.apache.org/documentation.html#connect)
for loading data from Kafka to MemSQL.

## Configuration

The `memsql-kafka-connector` is configurable via property file that should be
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
| `tableKey.<index_type>[.name]`       | Specify additional keys to add to tables created by the connector; value of this property is the comma separated list with names of the columns to apply key; <index_type> one of (`PRIMARY`, `COLUMNSTORE`, `UNIQUE`, `SHARD`, `KEY`);
| `memsql.loadDataCompression`         | Compress data on load; one of (`GZip`, `LZ4`, `Skip`) (default: GZip)
| `memsql.metadata.allow`              | Allows or denies the use of an additional meta-table to save the recording results (default: true)
| `memsql.metadata.table`              | Specify the name of the table to save kafka transaction metadata (default: `kafka-connect-transaction-metadata`)

### Config example
```
{
    "name": "memsql-sink-connector",
    "config": {
        "connector.class":"com.memsql.kafka.MemSQLSinkConnector",
        "tasks.max":"1",
        "topics":"topic-test-1,topic-test-2",
        "connection.ddlEndpoint" : "memsql-host1:3306",
        "connection.dmlEndpoints" : "memsql-host2:3306,memsql-host3:3306",
        "connection.database" : "test",
        "connection.user" : "root",
        "params.connectTimeout" : "10000"
        "params.ssl" : "false",
        "tableKey.primary.keyName" : "id",
        "tableKey.key.keyName" : "`col1`, col2, `col3`",
        "memsql.loadDataCompression" : "LZ4"
    }
}
```

##Auto-creation of tables

If the table does not exist, it will be created using the information from the first record.

The table name is the name of the topic. The table schema is taken from record valueSchema.
if valueSchema is not a struct, then a single column with name `data` will be created with the schema of the record.
Table keys are taken from tableKey option.

If the table already exists, all records will be loaded directly into it.
The auto-evolution of the table is not supported yet (all records should have the same schema).

##Exactly once delivery

To achieve exactly once delivery you could set `memsql.metadata.allow` to true.
Then `kafka-connect-transaction-metadata` table will be created.
This table contains an identifier, count of records, and time of each transaction.

The name of this table could be overwritten with `memsql.metadata.table` option.

## Data Types

`memsql-kafka-connector` makes such conversions from Kafka types to MemSQL types:

| Kafka Type    | MemSQL Type
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

To be able to add some column as a key in MemSQL you could use `tableKey` parameter like this:

Suppose you have an entity
```
{
    "id" : 123,
    "name" : "Alice"
}
```

and you want to add `id` column as a `PRIMARY KEY` to your MemSQL table. Then you could add
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
