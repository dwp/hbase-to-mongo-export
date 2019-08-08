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
                connection.create_table(args.topic_list_table,
                                        {'cf': dict(max_versions=100)})
                connection.create_table(args.data_table,
                                        {'cf': dict(max_versions=100)})


            topic_list_table = connection.table(args.topic_list_table)
            data_table = connection.table(args.data_table)
            connected = True

            if args.data_key_service:
                content = requests.get(f'{args.data_key_service}/datakey').json()
                encryption_key = content['plaintextDataKey']
                encrypted_key = content['ciphertextDataKey']
                master_key_id = content['dataKeyEncryptionKeyId']
            else:
                encryption_key = "czMQLgW/OrzBZwFV9u4EBA=="
                master_key_id = "1234567890"
                encrypted_key = "blahblah"

            with (open(args.test_configuration_file)) as file:
                data = json.load(file)
                for datum in data:
                    record_id = datum['kafka_message_id']
                    timestamp = datum['kafka_message_timestamp']
                    db_name = datum['db']
                    collection_name = datum['collection']
                    topic_name = "db." + db_name + "." + collection_name

                    value = datum['value']
                    print("Creating record %s timestamp %s topic %s".format(record_id, timestamp, topic_name))

                    if 'dbObject' in value:
                        db_object = value['dbObject']
                        if db_object != "CORRUPT":
                            value['dbObject'] = ""
                            record = unique_decrypted_db_object()
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

                        collumn_family = "topic:%s".format(topic_name)
                        obj = {collumn_family: json.dumps(value)}
                        data_table.put(record_id, obj, timestamp=int(timestamp))

                        print("Saved record %s timestamp %s topic %s".format(record_id, timestamp, topic_name))
                    else:
                        print("Skipped record %s as dbObject was missing".format(record_id))

                if args.dump_table_contents:
                    for key, data in data_table.scan():
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


def decrypted_db_object():
    return {
        "_id": {
            "declarationId": "RANDOM_GUID"
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


def unique_decrypted_db_object():
    record = decrypted_db_object()
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
    parser.add_argument('-k', '--data-key-service',
                        help='Use the specified data key service.')
    parser.add_argument('-o', '--remove-output-file',
                        help='Remove the output file.')
    parser.add_argument('-s', '--skip-table-creation', action='store_true',
                        help='Do not create the target table.')
    parser.add_argument('-t', '--destination-table', default='ucdata',
                        help='The table to write the records to.')
    parser.add_argument('-z', '--zookeeper-quorum', default='hbase',
                        help='The zookeeper quorum host.')
    parser.add_argument('test_configuration_file',
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
