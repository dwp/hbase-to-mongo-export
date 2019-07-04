#!/usr/bin/env python3

import happybase

connection = happybase.Connection('localhost')
connection.open()
connection.create_table('ucdata', {'cf': dict(max_versions=10),})
print(connection.tables());
