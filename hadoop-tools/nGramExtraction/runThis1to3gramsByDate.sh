#!/bin/bash

if [ $# -ne 6 ]
then
    echo "./runThis1to3gramsByDate.sh INPUT OUTPUT MESSAGE_FIELD GROUP_ID DATE_ID DATE_REGEX"
    exit 1
fi

for n in `seq 1 3`; do
    name=`basename $2`
    path=`dirname $2`
    if hdfs dfs -test -e "${path}/feat.${n}gram.${name}.date"; then
	echo "The directory already exists"
    else
	./runThisByGroupByDate.sh "$1" "${path}/feat.${n}gram.${name}.date" "$3" "$4" "$n" "$5" "$6"
    fi
done
