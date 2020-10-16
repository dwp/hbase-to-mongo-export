#!/usr/bin/env python3

import argparse
import base64
import binascii
import json
import sys

import happybase
import requests
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter


def main():
    args = command_line_args()
    connection = happybase.Connection(args.zookeeper_quorum)
    connection.open()
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']
    tables = [x.decode('ascii') for x in connection.tables()]

    table_name = "database:collection"

    if table_name not in tables:
        connection.create_table(table_name, {'cf': dict(max_versions=1000000)})
        print(f"Created table '{table_name}'.")

    table = connection.table(table_name)
    batch = table.batch()
    for i in range(int(args.records)):
        wrapper = kafka_message(i)
        record = decrypted_db_object(i)
        record_string = json.dumps(record)
        [iv, encrypted_record] = encrypt(encryption_key, record_string)
        wrapper['message']['encryption']['initialisationVector'] = iv.decode('ascii')
        wrapper['message']['encryption']['keyEncryptionKeyId'] = master_key_id
        wrapper['message']['encryption']['encryptedEncryptionKey'] = encrypted_key
        wrapper['message']['dbObject'] = encrypted_record.decode('ascii')
        message_id = json.dumps(wrapper['message']['_id'])
        checksum = binascii.crc32(message_id.encode("ASCII"), 0).to_bytes(4, sys.byteorder)
        hbase_id = checksum + message_id.encode("utf-8")
        obj = {'cf:record': json.dumps(wrapper)}
        batch.put(hbase_id, obj)
    batch.send()

def encrypt(key, plaintext):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(key), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector),
            base64.b64encode(ciphertext))


def kafka_message(i: int):
    return {
        "traceId": f"{i:05d}",
        "unitOfWorkId": f"{i:05d}",
        "@type": "V4",
        "message": {
            "db": "database",
            "collection": "collection",
            "_id": {
                "record_id": f"{i:05d}"
            },
            "_timeBasedHash": "hash",
            "@type": "MONGO_INSERT",
            "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
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


def decrypted_db_object(i: int):
    return {
        "_id": {
            "record_id": f"{i:05d}"
        },
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
        "createdDateTime": "2015-03-20T12:23:25.183Z",
        "_version": 2,
        "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
    }


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase for profiling.')
    parser.add_argument('-k', '--data-key-service', default='http://dks-standalone-http:8080/datakey',
                        help='Use the specified data key service.')
    parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                        help='The zookeeper quorum host.')
    parser.add_argument('-r', '--records', default='10000',
                        help='The number of records to create.')
    return parser.parse_args()


if __name__ == "__main__":
    main()
