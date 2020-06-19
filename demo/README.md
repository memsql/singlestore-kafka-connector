# memsql-kafka-connector demo

This demo will show basic functionality of `memsql-kafka-connector`

## Requirements

* docker
* maven

## Set up environment

To be able to run setup script you should first export MemSQL License Key like this:

```
export LICENSE_KEY=<memsql_license_key>
```

Then run the setup script:

```
./setup-script.sh
```

This script will start all required components: 
* zookeeper 
* kafka 
* schema-registry 
* kafka-connect
* memsql

And start kafka-connect job with such configuration:

```
{
    "name": "memsql-sink-connector",
    "config": {
        "connector.class":"com.memsql.kafka.MemSQLSinkConnector",
        "tasks.max":"1",
        "topics":"memsql-json-songs",
        "connection.ddlEndpoint" : "memsql-kafka:3306",
        "connection.database" : "test",
        "connection.user" : "root"
    }
 }
```

This job will read `memsql-json-songs` topic 
and then write all records to MemSQL `test.memsql-json-songs` table

To ingest some data to `memsql-json-songs` topic you can execute `ingest-data.sh` script, 
which will add some example data to kafka.

## Ingest data

```
./ingest-data.sh
```

After that you can see that the data has been added to the database

```
docker exec -it memsql-kafka bash
memsql

use test;
show tables;
select * from `memsql-json-songs`;
```