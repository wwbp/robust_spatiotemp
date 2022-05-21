#!/bin/bash

table=""



ngramExtraction () {
    table="$1"
    path="/tmp/maarten/rndTwt_byMonth"
    echo "#####################################################################"
    echo $table
    if hdfs dfs -test -e "${path}/${table}.cnty.csv"; then
	echo ./runThis1to3gramsByDate.sh "${path}/${table}.cnty.csv" "${path}/${table}.cnty" 4 19 5 '^(20\d\d-\d\d-\d\d).+'
    fi
}


for j in 1 2 3;
do
    for i in `seq 1 9`;
    do
	table="messages_201${j}_0$i"
	ngramExtraction $table
    done
    for i in `echo 0 1 2`;
    do
        table="messages_201${j}_1$i"
	ngramExtraction $table
    done
done
