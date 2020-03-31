#!/usr/bin/env python3

import argparse
import base64
import binascii
import json
import os
import time
import uuid
import math
import random
import happybase
import requests
import thriftpy2

from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter

def main():
    args = command_line_args()
    connected = False
    attempts = 0
    connection = happybase.Connection(args.zookeeper_quorum)
    connection.open()
    topics_table = connection.table(args.topics_table_name)
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']
    tables = [x.decode('ascii') for x in connection.tables()]
    table_cache = {}
    print(f"tables: '{tables}'")
    created = {}
    for i in range(int(args.records)):
        datum = kafka_message(i)
        salt = math.floor(random.random() * 256).to_bytes(length=1, byteorder='big')
        record_id = bytearray(datum['kafka_message_id'].encode('ascii'))
        db_id = bytes(salt + record_id)
        timestamp = datum['kafka_message_timestamp']
        value = datum['kafka_message_value']
        db_name = value['message']['db']
        collection_name = value['message']['collection']
        topic_name = "db." + db_name + "." + collection_name
        record = unique_decrypted_db_object()
        record_string = json.dumps(record)
        [iv, encrypted_record] = encrypt(encryption_key, record_string)
        value['message']['encryption']['initialisationVector'] = iv.decode('ascii')
        value['message']['encryption']['keyEncryptionKeyId'] = master_key_id
        value['message']['encryption']['encryptedEncryptionKey'] = encrypted_key
        value['message']['dbObject'] = encrypted_record.decode('ascii')
        table_name = db_name + ":" + collection_name

        if not table_name in tables:
            connection.create_table(table_name, {'cf': dict(max_versions=1000000)})
            tables.append(table_name)
            print(f"Created table '{table}'.")

        obj = {'cf:record': json.dumps(value)}
        table = table_cache[table_name] if table_name in table_cache \
            else connection.table(table_name)

        table_cache[table_name] = table
        table.put(db_id, obj) #, timestamp=int(timestamp))
        print("Saved record '{}' in table '{}'.".format(db_id, table_name))


def encrypt(key, plaintext):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(key), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector),
            base64.b64encode(ciphertext))


def kafka_message(i):

    return  {
     "kafka_message_id": "%08d" % i,
     "kafka_message_timestamp": "1",
     "kafka_message_value": {
         "traceId": f"{guid()}",
         "unitOfWorkId": f"{guid()}",
         "@type": "V4",
         "message": {
             "db": "quartz",
             "collection": "claimantEvent",
             "_id": {
                 "claimantEventId": f"{guid()}"
             },
             "_timeBasedHash": "hash",
             "@type": "MONGO_INSERT",
             "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
             "encryption": {
                 "encryptionKeyId": "",
                 "encryptedEncryptionKey": "",
                 "initialisationVector": "",
                 "keyEncryptionKeyId": ""
             },
             "dbObject": ""
         },
         "version": "core-4.master.9790",
         "timestamp": "2019-07-04T07:27:35.104+0000"
     }
    }

def decrypted_db_object():
    return {
        "_id": "qwertyuiop",
        "type": "addressDeclaration",
        "contractId": "RANDOM_GUID",
        "addressNumber": {
            "type": "AddressLine",
            "cryptoId": "RANDOM_GUID"
        },
        "addressLine2": None,
        "townCity": {
            "type": "AddressLine",
            "cryptoId": "RANDOM_GUID"
        },
        "postcode": "SM5 2LE",
        "processId": "RANDOM_GUID",
        "effectiveDate": {
            "type": "SPECIFIC_EFFECTIVE_DATE",
            "date": 20150320,
            "knownDate": 20150320
        },
        "paymentEffectiveDate": {
            "type": "SPECIFIC_EFFECTIVE_DATE",
            "date": 20150320,
            "knownDate": 20150320
        },
        "createdDateTime": {
            "$date":"2015-03-20T12:23:25.183Z",
            "_archivedDateTime":"should be replaced by _archivedDateTime"
        },
        "_version": 2,
        "_archived":"should be replaced by _removed",
        "unicodeNull": "\u0000",
        "unicodeNullwithText": "some\u0000text",
        "lineFeedChar": "\n",
        "lineFeedCharWithText": "some\ntext",
        "carriageReturn": "\r",
        "carriageReturnWithText": "some\rtext",
        "carriageReturnLineFeed": "\r\n",
         "carriageReturnLineFeedWithText": "some\r\ntext",
        "_lastModifiedDateTime": {
            "$date": "2018-12-14T15:01:02.000+0000"
        }
    }


def guid():
    return str(uuid.uuid4())

def unique_decrypted_db_object():
    record = decrypted_db_object()
    record['contractId'] = 1234
    record['addressNumber']['cryptoId'] = 1234
    record['townCity']['cryptoId'] = 1234
    record['processId'] = 1234
    return record


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase for profiling.')
    parser.add_argument('-k', '--data-key-service',default='http://dks-standalone-http:8080/datakey',
                        help='Use the specified data key service.')
    parser.add_argument('-dt', '--data-table-name', default='ucfs-data',
                        help='The data table to write the records to.')
    parser.add_argument('-tt', '--topics-table-name', default='ucfs-topics',
                        help='The table to write the list of topics to.')
    parser.add_argument('-z', '--zookeeper-quorum', default='localhost',
                        help='The zookeeper quorum host.')
    parser.add_argument('-r', '--records', default='1000',
                        help='The number of records to create.')
    return parser.parse_args()


if __name__ == "__main__":
    main()
