# Quickstart singlestore-kafka-connector guide

This guide will show basic functionality of `singlestore-kafka-connector`

## Requirements

* docker

## Set up environment

To be able to run the setup script, you must first export your SingleStore license key as follows:

```
export LICENSE_KEY=<singlestore_license_key>
```

Then run the setup script:

```
./setup-script.sh
```

This script will start all the required components: 
* zookeeper 
* kafka 
* schema-registry 
* kafka-rest
* kafka-connect
* singlestore (with user `root` and password `root`)

Then the kafka-connect job will be launched with this configuration:

```
{
    "name": "singlestore-sink-connector",
    "config": {
        "connector.class":"com.singlestore.kafka.SingleStoreSinkConnector",
        "tasks.max":"1",
        "topics":"singlestore_json_songs",
        "connection.ddlEndpoint" : "singlestore-kafka:3306",
        "connection.database" : "test",
        "connection.user" : "root"
    }
 }
```

This job will read `singlestore_json_songs` topic 
and then write all records to SingleStore `test.singlestore_json_songs` table

## Ingest data

To ingest some data to `singlestore_json_songs` topic you can execute `ingest-data.sh` script, 
which will add some sample data to kafka.

```
./ingest-data.sh
```

After that, you will see that the data has been added to the database.

```
docker exec -it singlestore-kafka bash
memsql -u root -proot

use test;
show tables;
select * from singlestore_json_songs;
```