#!/bin/bash

hbase_name=$(docker exec hbase cat /etc/hosts \
                 | egrep -v '(localhost|ip6)' | tail -n1)

if [[ -n "$hbase_name" ]]; then
    temp_file=$(mktemp)

    (
        cat /etc/hosts | grep -v 'added by hbase-to-mongo-export.$'
        echo $hbase_name \# added by hbase-to-mongo-export.
    ) > $temp_file

    mv $temp_file /etc/hosts
    chmod 644 /etc/hosts
    cat /etc/hosts
else
    (
        echo could not get host name from hbase hosts file:
        docker exec hbase cat /etc/hosts
    ) >&2
fi
