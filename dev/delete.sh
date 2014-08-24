#!/bin/sh

if [ "$1" == "" -o "$2" == "" ]; then
    echo "Please provide an entity name and an identity."
    exit 1
fi

curl -s -X DELETE http://127.0.0.1:4001/v2/keys/entities/$1/$2 && echo
