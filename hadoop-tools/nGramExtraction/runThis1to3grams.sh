#!/bin/bash

if [ $# -ne 4 ]
then
    echo "./runThis1to3grams.sh INPUT OUTPUT MESSAGE_FIELD GROUP_ID"
    exit 1
fi

for n in `seq 1 3`; do
    name=`basename $2`
    path=`dirname $2`
    ./runThis.sh "$1" "${path}/feat.${n}gram.${name}" "$3" "$4" "$n"
done
