#!/bin/bash

main() {
    make_keystore dks-keystore.jks dks
    extract_public_certificate dks-keystore.jks dks.crt
    make_truststore dks-truststore.jks dks.crt

    make_keystore keystore.jks hbase-to-mongo-export
    extract_public_certificate keystore.jks hbase-to-mongo-export.crt
    make_truststore truststore.jks hbase-to-mongo-export.crt

    make_keystore hbase-init-keystore.jks hbase-init
    extract_public_certificate hbase-init-keystore.jks hbase-init.crt
    import_into_truststore dks-truststore.jks hbase-init.crt hbase-init

    make_keystore integration-tests-keystore.jks integration-tests
    extract_public_certificate integration-tests-keystore.jks integration-tests.crt
    make_truststore integration-tests-truststore.jks integration-tests.crt
    import_into_truststore integration-tests-truststore.jks dks.crt dks

    import_into_truststore dks-truststore.jks integration-tests.crt integration-tests
    import_into_truststore dks-truststore.jks hbase-to-mongo-export.crt hbase-to-mongo-export
    import_into_truststore truststore.jks dks.crt dks

    extract_pems ./hbase-init-keystore.jks
    extract_pems ./dks-keystore.jks

    cp -v dks-crt.pem hbase-init-key.pem hbase-init-crt.pem images/hbase

    mv -v dks-truststore.jks images/dks
    mv -v dks-keystore.jks images/dks
    cp -v truststore.jks images/htme
    cp -v keystore.jks images/htme
    cp -v integration-tests-keystore.jks images/tests
    cp -v integration-tests-truststore.jks images/tests
}

make_keystore() {
    local keystore="${1:?Usage: $FUNCNAME keystore common-name}"
    local common_name="${2:?Usage: $FUNCNAME keystore common-name}"

    [[ -f "${keystore}" ]] && rm -v "${keystore}"

    keytool -v \
            -genkeypair \
            -keyalg RSA \
            -alias cid \
            -keystore "${keystore}" \
            -storepass $(password) \
            -validity 365 \
            -keysize 2048 \
            -keypass $(password) \
            -dname "CN=${common_name},OU=DataWorks,O=DWP,L=Leeds,ST=West Yorkshire,C=UK"
}

extract_public_certificate() {
    local keystore="${1:?Usage: $FUNCNAME keystore certificate}"
    local certificate="${2:?Usage: $FUNCNAME keystore certificate}"

    [[ -f "${certificate}" ]] && rm -v "${certificate}"

    keytool -v \
            -exportcert \
            -keystore "${keystore}" \
            -storepass $(password) \
            -alias cid \
            -file "$certificate"
}

make_truststore() {
    local truststore="${1:?Usage: $FUNCNAME truststore certificate}"
    local certificate="${2:?Usage: $FUNCNAME truststore certificate}"
    [[ -f ${truststore} ]] && rm -v "${truststore}"
    import_into_truststore ${truststore} ${certificate} self
}

import_into_truststore() {
    local truststore="${1:?Usage: $FUNCNAME truststore certificate}"
    local certificate="${2:?Usage: $FUNCNAME truststore certificate}"
    local alias="${3:-cid}"

    keytool -importcert \
            -noprompt \
            -v \
            -trustcacerts \
            -alias "${alias}" \
            -file "${certificate}" \
            -keystore "${truststore}" \
            -storepass $(password)
}

extract_pems() {
    local keystore=${1:-keystore.jks}
    local key=${2:-${keystore%-keystore.jks}-key.pem}
    local certificate=${3:-${keystore%-keystore.jks}-crt.pem}

    local intermediate_store=${keystore/jks/p12}

    local filename=$(basename $keystore)
    local alias=cid

    [[ -f $intermediate_store ]] && rm -v $intermediate_store
    [[ -f $key ]] && rm -v $key

    if keytool -importkeystore \
               -srckeystore $keystore \
               -srcstorepass $(password) \
               -srckeypass $(password) \
               -srcalias $alias \
               -destalias $alias \
               -destkeystore $intermediate_store \
               -deststoretype PKCS12 \
               -deststorepass $(password) \
               -destkeypass $(password); then
        local pwd=$(password)
        export pwd

        openssl pkcs12 \
                -in $intermediate_store \
                -nodes \
                -nocerts \
                -password env:pwd \
                -out $key

        openssl pkcs12 \
                -in $intermediate_store \
                -nokeys \
                -out $certificate \
                -password env:pwd

        unset pwd
    else
        echo Failed to generate intermediate keystore $intermediate_store >&2
    fi
}

password() {
    echo changeit
}

main
