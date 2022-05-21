#!/bin/bash

table=""
path="/tmp/maarten/rndTwt_byMonth"

for j in 2 3;
do
    for i in `seq 1 9`;
    do
	table="messages_201${j}_0$i"
	echo "#####################################################################"
	echo $table $n
	if hdfs dfs -test -e "${path}/${table}.cnty.csv"; then
	    ./runThis1to3grams.sh "${path}/${table}.cnty.csv" "${path}/feats2/${table}.cnty" 4 19
	fi
	
    done
    for i in `echo 0 1 2`;
    do
        table="messages_201${j}_1$i"
	echo "#####################################################################"
	echo $table $n
	if hdfs dfs -test -e "${path}/${table}.cnty.csv"; then
	    ./runThis1to3grams.sh "${path}/${table}.cnty.csv" "${path}/feats2/${table}.cnty" 4 19
	fi
    done
done
