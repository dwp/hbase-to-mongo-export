# hbase-crown-export

Selects the latest records from a single hbase table and writes them out in
mongo backup format, i.e. 1 json record per line.

## Requirements

1. JDK 8+
2. Docker
3. docker-compose
4. Python 3.7
5. virtualenv

## Run locally

### In an IDE or not containerized (with containerized hbase)

This is slightly cumbersome as zookeeper gives the docker network name of the
hbase which can't be resolved outside of docker, however this can be remedied
if the name given by zooker is then entered into the local ```/etc/hosts```
file.

1. Bring up the hbase container:

    docker-compose up -d hbase

2. Populate local hosts file:

    sudo ./scripts/hosts.sh

3. Populate local instance with sample data

    1. (Optional) activate a local python environment
       ```
       cd scripts
       virtualenv -p $(which python3) environment
       . ./environment/bin/activate
       pip3 install -r ./requirements.txt
       ```
    2. Populate the instance (from the ```scripts``` directory):
       ```
       ./populate.py -dz localhost ./sample-data.json
       ```
It should now be possible to run code in an IDE against the local instance.

* The main class is
  ```app.HBaseCrownExport```

* The program arguments are
  ```
  --source.table.name=ucdata
  --hbase.zookeeper.quorum=localhost
  --hbase.crown.export.file.output=data/output.txt
  ```
* The active spring profiles are (-Dspring.profiles.active=)
  ```phoneyServices,localDataSource,outputFile```

### Run locally containerized
    HBASE_CROWN_EXPORT_VERSION=$(cat ./gradle.properties | cut -f2 -d'=') \
        docker-compose up --build -d hbase hbase-populate hbase-crown-export

### Additionally run the integration tests against local containerized setup
    docker-compose build hbase-crown-itest
    docker-compose run hbase-crown-itest
