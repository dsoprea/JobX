#!/bin/sh

if [ "$1" == "" ]; then
    echo "Please provide a request-ID."
    exit 1
fi

dot -Tpng -o request-$1.png request-$1.dot
