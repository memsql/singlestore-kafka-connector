# SingleStore Kafka Connector

## [![Maven Central](https://img.shields.io/maven-central/v/com.singlestore/singlestore-kafka-connector?label=Version)](https://mvnrepository.com/artifact/com.singlestore/singlestore-kafka-connector) [![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

## Getting Started

SingleStore Kafka Connector is a [kafka-connector](http://kafka.apache.org/documentation.html#connect)
for loading data from Kafka to SingleStore.

Quickstart guide can be found [here](https://github.com/memsql/singlestore-kafka-connector/blob/master/demo/README.md)

You can find the latest version of the connector on [Maven](https://mvnrepository.com/artifact/com.singlestore/singlestore-kafka-connector).

## Configuration

The `singlestore-kafka-connector` is configurable via property file that should be
specified before starting kafka-connect job.

| Option                                                      | Description                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection.ddlEndpoint` (Self-Managed deployment) (required) | The hostname or IP address of the SingleStore Master Aggregator in the `host[:port]` format, where `port` is an optional parameter. Example: `master-agg.foo.internal:3308` or `master-agg.foo.internal`.                                                                |
| `connection.dmlEndpoints` (Self-Managed deployment)           | A comma-separated list of the hostname or IP address of SingleStore Aggregator nodes to run queries against in the `host[:port],host[:port],...` format, where `port` is an optional parameter. Example: `child-agg:3308,child-agg2`. Default: `ddlEndpoint`. |
| `connection.clientEndpoint` (Helios deployment) (required)   | The hostname or IP address of the SingleStore Helios workspace to run queries against in the `host[:port]` format, where `port` is an optional parameter. Example: `svc-XXXX-ddl.aws-oregon-2.svc.singlestore.com:3306`.                                                  |
| `connection.database` (required)                            | If set, all connections will default to using this database (default: empty)                                                                                                                                                                                               |
| `connection.user`                                           | SingleStore username (default: `root`)                                                                                                                                                                                                                                     |
| `connection.password`                                       | SingleStore password (default: no password)                                                                                                                                                                                                                                |
| `params.<name>`                                             | Specify a specific MySQL or JDBC parameter which will be injected into the connection URI (default: empty)                                                                                                                                                                 |
| `max.retries`                                               | The maximum number of times to retry on errors before failing the task. (default: 10)                                                                                                                                                                                      |
| `fields.whitelist`                                          | Comma-separated list of record fields to insert. When empty, all fields are candidates unless excluded by `fields.blacklist`.                                                                                                                                                                                            |
| `fields.blacklist`                                          | Comma-separated list of top-level fields to omit from the row (e.g. columns with `DEFAULT` / `ON UPDATE`). Applied after the whitelist when both are set. (default: none excluded)                                                                                                                                                                                            |
| `retry.backoff.ms`                                          | The time in milliseconds to wait following an error before a retry attempt is made. (default 3000)                                                                                                                                                                         |
| `tableKey.<index_type>[.name]`                              | Specify additional keys to add to tables created by the connector; value of this property is the comma separated list with names of the columns to apply key; <index_type> one of (`PRIMARY`, `COLUMNSTORE`, `UNIQUE`, `SHARD`, `KEY`);                                    |
| `custom.metric.tags`                                        | Comma-separated list of key-value pairs (`key=value`) added as extra JMX tags to task metric ObjectNames. Example: `team=data,env=prod`.                                                                                                                                  |
| `singlestore.loadDataCompression`                           | Compress data on load; one of (`GZip`, `LZ4`, `Skip`) (default: GZip)                                                                                                                                                                                                      |
| `singlestore.metadata.allow`                                | Allows or denies the use of an additional meta-table to save the recording results (default: true)                                                                                                                                                                         |
| `singlestore.metadata.table`                                | Specify the name of the table to save kafka transaction metadata (default: `kafka_connect_transaction_metadata`)                                                                                                                                                           |
| `singlestore.tableName.<topicName>=<tableName>`             | Specify an explicit table name to use for the specified topic                                                                                                                                                                                                              |

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

| Kafka Type | SingleStore Type |
| ---------- | ---------------- |
| STRUCT     | JSON             |
| MAP        | JSON             |
| ARRAY      | JSON             |
| INT8       | TINYINT          |
| INT16      | SMALLINT         |
| INT32      | INT              |
| INT64      | BIGINT           |
| FLOAT32    | FLOAT            |
| FLOAT64    | DOUBLE           |
| BOOLEAN    | TINYINT          |
| BYTES      | TEXT             |
| STRING     | VARBINARY(1024)  |

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

## Mutual TLS (mTLS) Configuration

This guide explains how to configure an mTLS connection for the **SingleStore Kafka Connector** on **SingleStore Helios**.

### 1. Generate Client Certificates

First, generate the necessary certificates for SingleStore mTLS connections. Follow the steps outlined in the [SingleStore Documentation](https://docs.singlestore.com/db/v9.0/connect-to-singlestore/connect-to-singlestore-using-tls-ssl/#generate-client-certificates-for-singlestore-mtls-connections).

Upon completion, you should have the following files:
* `ca-key.pem` – The private key for your CA.
* `ca-cert.pem` – The public CA certificate.
* `client-key.pem` – The client private key.
* `client-cert.pem` – The client certificate signed by the CA.


Additionally, download the SingleStore server certificate bundle: [`singlestore_bundle.pem`](https://portal.singlestore.com/static/ca/singlestore_bundle.pem).

### 2. Create a Java Keystore
The connector requires the client certificate in a keystore format. Run the following command to create a `client-keystore.p12` file, ensuring you provide the correct file paths and a secure password:

```bash
openssl pkcs12 -export \
  -inkey /path/to/client-key.pem \
  -in /path/to/client-cert.pem \
  -out client-keystore.p12 \
  -name client-cert \
  -CAfile /path/to/ca-cert.pem \
  -caname root \
  -passout pass:<your_keystore_password>
```

This command should create `client-keystore.p12` file.

### 3. Configure SingleStore Helios

To enable mTLS, you must upload your CA certificate to the SingleStore Portal and create a database user that requires X.509 authentication.

1.  Log in to the **SingleStore Portal**.
2.  Navigate to **Workspace > Security**.
3.  Locate the **Upload a CA Bundle** section and upload your `ca-cert.pem` file.

### 4. Create mTLS User

Connect to your SingleStore deployment and run the following SQL commands. 

*Note: Replace `<mtls_password>` with a secure password and update the `GRANT` statement with the specific permissions required for your workflow.*

```sql
CREATE USER 'mtls_user'@'%' IDENTIFIED BY '<mtls_password>' REQUIRE X509;

-- Example: Grant all privileges (adjust as needed for least privilege)
GRANT ALL PRIVILEGES ON *.* TO 'mtls_user'@'%';
```

### 5. Update Connector Configuration

Finally, update your Kafka Connector JSON configuration to enable the mTLS connection. Ensure the following:

* **File Paths:** The paths for `serverSslCert` and `keystore` must point to the correct locations on your worker nodes.
* **Passwords:** Replace `<mtls_password>` and `<keystore_password>` with your actual secure values.
* **SSL Mode:** Set `<ssl_mode>` to either `verify-full` (encryption, certificate validation and hostname verification) or `verify-ca` (encryption, certificates validation, BUT no hostname verification) based on your security requirements. For more information and a complete list of available SSL/TLS configuration options, refer to the [SingleStore JDBC Driver - TLS Parameters](https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#tls-parameters) documentation.

```
"config": {
    ...
    "connection.user" : "mtls_user",
    "connection.password" : "<mtls_password>",
    "params.sslMode" : "<ssl_mode>",
    "params.serverSslCert": "/path/to/singlestore_bundle.pem",
    "params.keystore": "/path/to/client-keystore.p12",
    "params.keyStorePassword": "<keystore_password>",
    "params.keyStoreType": "PKCS12",
    ...
}
```


## Metrics

The connector exports task-level metrics via JMX.

### Metric name

Each task registers a JMX MBean with the following ObjectName format:

`singlestore.kafka:type=connector-metrics,context=sink,connector-name=<connector-name>,task=<task-id>[,<custom-tags>]`

- `connector-name` comes from the Kafka Connect connector `name` property.
- `task` is the Kafka Connect task id (`0`, `1`, ...).
- optional `custom-tags` come from `custom.metric.tags` configuration.

### Exported metrics

The connector currently exports these task metrics:

- `TaskStatus`: current task state. Values are `unassigned`, `running`, `failed`, or `stopped`.
- `RecordsProcessedTotal`: cumulative number of Kafka records accepted by the task and successfully processed by `put(...)`.
- `WriteErrorsTotal`: cumulative number of write attempts that failed.

### Lifecycle behavior

Metrics are exported per task and follow the task lifecycle:

1. metric MBean is registered when a task starts
2. task status is updated while the task runs
3. metric MBean is unregistered when the task stops


## Setting up development environment

- clone the repository https://github.com/memsql/singlestore-kafka-connector.git
- open a project with Intellij IDEA
- to run unit tests use the `unit-tests` run configuration
- before running integration tests, start [MemSQL CIAB](https://hub.docker.com/r/memsql/cluster-in-a-box) cluster using the `setup-cluster` run configurations
- to run integration tests use the `integration-tests` run configuration

## Release process

To release a new version:

1. Push a version tag using semantic versioning with a `v` prefix (`v<major>.<minor>.<patch>`, for example `v1.2.3`):

   ```bash
   git tag v1.0.1
   git push origin v1.0.1
   ```

   The package version is derived from the tag (the leading `v` is stripped). This triggers the [Release workflow](.github/workflows/release.yml), which:

   - Builds the package
   - Creates a [GitHub Release](https://github.com/memsql/singlestore-kafka-connector/releases) with auto-generated release notes and a Confluent Hub archive (`singlestore-singlestore-kafka-connector-<version>.zip`)
   - Publishes the package to [Maven Central](https://mvnrepository.com/artifact/com.singlestore/singlestore-kafka-connector)

2. After the GitHub Release is published, email [confluent-hub@confluent.io](mailto:confluent-hub@confluent.io) to update the connector on Confluent Hub. Replace `<version>` with the release version (for example, `1.2.9` for tag `v1.2.9`):

   ```
   Hi,

   We have updated the SingleStore Kafka Sink connector.
   You can access the updated connector at the following link:
   https://github.com/memsql/singlestore-kafka-connector/releases/download/v<version>/singlestore-singlestore-kafka-connector-<version>.zip

   Please let me know if you have any issues accessing the archive package or if any further steps are required to update the package on Confluent Hub.

   Best regards,
   <Your name>
   ```
