# hbase-to-mongo-export

Selects the latest records from a single hbase table and writes them out in
mongo backup format, i.e. 1 json record per line.

## Requirements

1. JDK 8+
2. Docker
3. docker-compose

## Run locally

### In an IDE or not containerized (with containerized hbase)

This is slightly cumbersome as zookeeper gives the docker network name of the
hbase host which can't be resolved outside of docker, however this can be remedied
if the name given by zooker is then entered into the local ```/etc/hosts```
file.

1. Bring up the hbase container:

    docker-compose up -d hbase hbase-populate

2. Add hbase entry in local /etc/hosts file:

    sudo ./scripts/hosts.sh

It should now be possible to run code in an IDE against the local instance.

* The main class is
  ```app.HBaseToMongoExport```

* The program arguments are
  ```
  --source.table.name=ucdata
  --hbase.zookeeper.quorum=localhost
  --file.output=data/output.txt
  ```
* The active spring profiles are (-Dspring.profiles.active=)
  ```phoneyServices,localDataSource,outputFile```

### Run locally containerized
    HBASE_TO_MONGO_EXPORT_VERSION=$(cat ./gradle.properties | cut -f2 -d'=') \
        docker-compose up --build -d hbase hbase-populate hbase-to-mongo-export

### Additionally run the integration tests against local containerized setup
    docker-compose build hbase-to-mongo-export-itest
    docker-compose run hbase-to-mongo-export-itest
