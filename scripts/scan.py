#!/usr/bin/env python3

import happybase
import json

connection = happybase.Connection('localhost')
connection.open()
claims_table = connection.table('ucdata')
print(claims_table)
for key, data in claims_table.scan():
    print(key, data)
