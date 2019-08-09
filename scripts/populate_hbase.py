#!/usr/bin/env python3

import argparse
import base64
import binascii
import json
import os
import time
import uuid

from pprint import pprint

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
    init(args)
    while not connected and attempts < 100:
        try:
            connection = happybase.Connection(args.zookeeper_quorum)
            connection.open()

            if not args.skip_table_creation:
                try:
                    connection.create_table(args.destination_table,
                                            {'cf': dict(max_versions=10)})
                except Exception as e:
                    print("Table may exist: {}".format(e))

            table = connection.table(args.destination_table)
            connected = True

            print("data_key_service='{}'".format(args.data_key_service))
            if args.data_key_service:
                content = requests.get(args.data_key_service).json()
                encryption_key = content['plaintextDataKey']
                encrypted_key = content['ciphertextDataKey']
                master_key_id = content['dataKeyEncryptionKeyId']
                print("Got data from dks")
            else:
                encryption_key = "czMQLgW/OrzBZwFV9u4EBA=="
                master_key_id = "1234567890"
                encrypted_key = "blahblah"
                print("Using fake dks data")

            with (open(args.sample_data_file)) as file:
                data = json.load(file)
                for datum in data:
                    record_id = datum['id']
                    timestamp = datum['cf:data']['timestamp']
                    value = datum['cf:data']['value']
                    if 'dbObject' in value:
                        db_object = value['dbObject']
                        if db_object != "CORRUPT":
                            del value['dbObject']
                            record = uniq_db_object()
                            record_string = json.dumps(record)
                            [iv, encrypted_record] = encrypt(encryption_key,
                                                             record_string)
                            value['encryption']['initialisationVector'] \
                                = iv.decode('ascii')

                            if master_key_id:
                                value['encryption']['keyEncryptionKeyId'] = \
                                    master_key_id

                            if encrypted_key:
                                value['encryption']['encryptedEncryptionKey'] = \
                                    encrypted_key

                            value['dbObject'] = encrypted_record.decode('ascii')
                        else:
                            value['encryption']['initialisationVector'] = "PHONEYVECTOR"

                        obj = {'cf:data': json.dumps(value)}
                        table.put(record_id, obj, timestamp=int(timestamp))

                if args.dump_table_contents:
                    for key, data in table.scan():
                        print(key, data)

                if args.completed_flag:
                    print("Creating directory: '{}'.".format(args.completed_flag))
                    os.makedirs(args.completed_flag)

        except (ConnectionError, thriftpy2.transport.TTransportException) as e:
            attempts = attempts + 1
            print("Failed to connect: '{}', attempt no {}.".format(e, attempts))
            time.sleep(3)

    exit(0 if connected else 1)


def encrypt(key, plaintext):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(key.encode("utf8"), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector),
            base64.b64encode(ciphertext))

def uc_object():
    return {
        "_id": {
            "declarationId": "aaaa1111-abcd-4567-1234-1234567890ab"
        },
        "type": "addressDeclaration",
        "contractId": "aa16e682-fbd6-4fe3-880b-118ac09f992a",
        "addressNumber": {
            "type": "AddressLine",
            "cryptoId": "bd88a5f9-ab47-4ae0-80bf-e53908457b60"
        },
        "addressLine2": None,
        "townCity": {
            "type": "AddressLine",
            "cryptoId": "9ca3c63c-cbfc-452a-88fd-bb97f856fe60"
        },
        "postcode": "SM5 2LE",
        "processId": "3b313df5-96bc-40ff-8128-07d496379664",
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
            "$date":"2015-03-20T12:23:25.183Z"
        },
        "_version": 2,
        "_lastModifiedDateTime": {
            "$date": "2018-12-14T15:01:02.000+0000"
        }
    }


def guid():
    return str(uuid.uuid4())


def uniq_db_object():
    record = uc_object()
    record['_id']['declarationId'] = guid()
    record['contractId'] = guid()
    record['addressNumber']['cryptoId'] = guid()
    record['townCity']['cryptoId'] = guid()
    record['processId'] = guid()
    return record


def command_line_args():
    parser = argparse.ArgumentParser(description='Pre-populate hbase.')
    parser.add_argument('-c', '--completed-flag',
                        help='The flag to write on successful completion.')
    parser.add_argument('-d', '--dump-table-contents', action='store_true',
                        help='Dump table contents after inserts.')
    parser.add_argument('-k', '--data-key-service-host',
                        help='Use the specified data key service.')
    parser.add_argument('-o', '--remove-output-file',
                        help='Remove the output file.')
    parser.add_argument('-s', '--skip-table-creation', action='store_true',
                        help='Do not create the target table.')
    parser.add_argument('-t', '--destination-table', default='ucdata',
                        help='The table to write the records to.')
    parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                        help='The zookeeper quorum host.')
    parser.add_argument('sample_data_file',
                        help='File containing the sample data.')
    return parser.parse_args()


def init(args):
    if args.completed_flag:
        if os.path.isdir(args.completed_flag):
            print("Removing directory '{}'.".format(args.completed_flag))
            os.removedirs(args.completed_flag)
        elif os.path.isfile(args.completed_flag):
            print("Removing file '{}'.".format(args.completed_flag))
            os.remove(args.completed_flag)
        else:
            print("Argument --completed-flag  was set but no file or folder to remove")
    else:
        print("Argument --completed-flag not set, no file removed")

    if args.remove_output_file:
        if os.path.isfile(args.remove_output_file):
            print("Removing file '{}'.".format(args.remove_output_file))
            os.remove(args.remove_output_file)
        else:
            print("Argument --remove-output-file not set, no file removed")


if __name__ == "__main__":
    main()
