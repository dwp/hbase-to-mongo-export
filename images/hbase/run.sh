#!/usr/bin/env bash

./hbase_data.py
./hbase_data.py --table "database:empty"
./hbase_data.py --table "data:equality"
