#!/bin/bash

hbase_name=$(docker exec hbase cat /etc/hosts \
                 | egrep -v '(localhost|ip6)' | tail -n1)

echo "hbase container is '${hbase_name}'"

if [[ -n "$hbase_name" ]]; then

    temp_file=$(mktemp)
    (
        cat /etc/hosts | grep -v 'added by hbase-to-mongo-export.$'
        echo ${hbase_name} local-hbase \# added by hbase-to-mongo-export.
    ) > $temp_file

    sudo mv $temp_file /etc/hosts
    sudo chmod 644 /etc/hosts
    cat /etc/hosts
else
    (
        echo could not get host name from hbase hosts file:
        docker exec hbase cat /etc/hosts
    ) >&2
fi
