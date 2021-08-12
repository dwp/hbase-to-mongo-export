#!/usr/bin/env python3

import argparse
import base64
import binascii
import json
import regex
import sys
import happybase
import requests
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter


def main():
    args = command_line_args()
    connection = happybase.Connection("hbase")
    connection.open()
    response = requests.get("https://dks:8443/datakey",
                            cert=("hbase-init-crt.pem",
                                  "hbase-init-key.pem"),
                            verify="dks-crt.pem")
    content = response.json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']
    table_name = args.table
    tables = [x.decode('ascii') for x in connection.tables()]

    if table_name not in tables:
        connection.create_table(table_name, {'cf': dict(max_versions=1000000)})
        print(f"Created table '{table_name}'.")

    table = connection.table(table_name)
    table_re = regex.compile(r"^(\w+):(\w+)$")
    match = table_re.search(table_name)
    database = match.group(1)
    collection = match.group(2)
    batch = table.batch(timestamp=1000)
    print("Creating batch.")
    for i in range(int(10000)):
        wrapper = kafka_message(i, database, collection)
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
    print("Sending batch.")
    batch.send()
    connection.close()
    print("Done.")


def encrypt(key, plaintext):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(key), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector),
            base64.b64encode(ciphertext))


def kafka_message(i: int, database: str, collection: str) -> dict:
    return {
        "traceId": f"{i:05d}",
        "unitOfWorkId": f"{i:05d}",
        "@type": "OUTER_TYPE",
        "message": {
            "db": database,
            "collection": collection,
            "_id": {
                "record_id": f"{i:05d}"
            },
            "_timeBasedHash": "hash",
            "@type": "INNER_TYPE",
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
        "_id": {"record_id": f"{i:05d}"} if i % 2 == 0 else f"{i:05d}",
        "createdDateTime": "2015-03-20T12:23:25.183Z",
        "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
    }


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase for profiling.')
    parser.add_argument('-t', '--table', default='database:collection',
                        help='The table to write to.')
    return parser.parse_args()


if __name__ == "__main__":
    main()
