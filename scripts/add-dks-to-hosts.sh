#!/bin/bash

dks_name=$(docker exec dks-standalone cat /etc/hosts \
                 | egrep -v '(localhost|ip6)' | tail -n1)

echo "dks container is '${dks_name}'"

if [[ -n "$dks_name" ]]; then

    temp_file=$(mktemp)
    (
        cat /etc/hosts | grep -v 'added by dks-to-mongo-export.$'
        echo ${dks_name} \# added by dks-to-mongo-export.
    ) > $temp_file

    sudo mv $temp_file /etc/hosts
    sudo chmod 644 /etc/hosts
    cat /etc/hosts
else
    (
        echo could not get host name from dks hosts file:
        docker exec dks cat /etc/hosts
    ) >&2
fi
