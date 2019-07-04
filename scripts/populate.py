#!/usr/bin/env python3

import happybase
import sys
import json
from pprint import pprint

connection = happybase.Connection('localhost')
connection.open()
#connection.create_table('claims', {'cf1': dict(max_versions=10),})
ucdata_table = connection.table('ucdata')

with (open(sys.argv[1])) as file:
     data = json.load(file)
     for datum in data:
          id = datum['id']
          timestamp  = datum['cf:data']['timestamp']
          value = datum['cf:data']['value']
          obj = { 'cf:data': json.dumps(value)}
          ucdata_table.put(id, obj, timestamp = int(timestamp))
