#!/bin/bash

# Getting working directory
WD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ $# -ne 5 ]
then
    echo "./runThis INPUT OUTPUT MESSAGE_FIELD GROUP_ID N"
    exit 1
fi

input="$1"
output="$2"
message_field="$3"
group_id="$4"
n="$5"

if hadoop fs -test -d "$output"
then
    echo "Already a directory: $output"
    exit
fi

source "$WD/classpath.sh"

cd "$WD/classes"

javac -d . ../ExtractNgrams.java || exit
rm ExtractNgrams.jar && jar -cvf ExtractNgrams.jar -C . .

echo hadoop jar ExtractNgrams.jar org.wwbp.ExtractNgrams -libjars "$libjars" -input "$input"  -output "$output" -message_field "$message_field" -group_id_index "$group_id" -n "$n"

hadoop jar ExtractNgrams.jar org.wwbp.ExtractNgrams \
    -libjars "$libjars" \
    -input "$input" \
    -output "$output" \
    -message_field "$message_field" \
    -group_id_index "$group_id" \
    -n "$n"
