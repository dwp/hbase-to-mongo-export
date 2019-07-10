#!/usr/bin/env python3

from pprint import pprint
import argparse
import happybase
import json
import os
import sys
import thriftpy2
import time

parser = argparse.ArgumentParser(description='Pre-populate hbase.')
parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                    help='The zookeeper quorum host.')
parser.add_argument('-c', '--completed-flag',
                    help='The flag to write on successful completion.')
parser.add_argument('-s', '--skip-table-creation', action='store_true',
                    help='Do not create the target table.')
parser.add_argument('-d', '--dump-table-contents', action='store_true',
                    help='Dump table contents after inserts.')
parser.add_argument('-o', '--prepare-output-file',
                    help='Prepare the output file.')
parser.add_argument('sample_data_file',
                    help='File containing the sample data.')

args = parser.parse_args()

connected = False
attempts = 0

if args.completed_flag and os.path.isdir(args.completed_flag):
     print("Removing directory '{}'.".format(args.completed_flag))
     os.removedirs(args.completed_flag)

if args.prepare_output_file:
     if os.path.isfile(args.prepare_output_file):
          print(f'Removing file {args.prepare_output_file}')
          os.remove(args.prepare_output_file)

while not(connected) and attempts < 100:
     try:
          connection = happybase.Connection(args.zookeeper_quorum)
          connection.open()

          if not(args.skip_table_creation):
               connection.create_table('ucdata', {'cf': dict(max_versions=10),})

          ucdata_table = connection.table('ucdata')
          connected = True
          with (open(args.sample_data_file)) as file:
               data = json.load(file)
               for datum in data:
                    id = datum['id']
                    timestamp  = datum['cf:data']['timestamp']
                    value = datum['cf:data']['value']
                    obj = { 'cf:data': json.dumps(value)}
                    ucdata_table.put(id, obj, timestamp = int(timestamp))

          if args.dump_table_contents:
               for key, data in ucdata_table.scan():
                    print(key, data)

          if args.completed_flag:
               print("Creating directory: '{}'.".format(args.completed_flag))
               os.makedirs(args.completed_flag)
     except (ConnectionError, thriftpy2.transport.TTransportException) as e:
          attempts = attempts + 1
          print("Failed to connect: '{}', attempt no {}.".format(e, attempts))
          time.sleep(3)

exit(0 if connected else 1)
