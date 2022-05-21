#!/bin/bash

# Getting working directory
WD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ $# -ne 7 ]
then
    echo "./runThis INPUT OUTPUT MESSAGE_FIELD GROUP_ID N DATE_INDEX DATE_REGEX"
    exit 1
fi

input="$1"
output="$2"
message_field="$3"
group_id="$4"
n="$5"
dateIndex="$6"
dateRegex="$7"

source "$WD/classpath.sh"

cd "$WD/classes"

javac -d . ../ExtractNgrams.java || exit
rm ExtractNgrams.jar && jar -cvf ExtractNgrams.jar -C . .

echo hadoop jar ExtractNgrams.jar org.wwbp.ExtractNgrams \
    -libjars "$libjars" \
    -input "$input" \
    -output "$output" \
    -message_field "$message_field" \
    -group_id_index "$group_id" \
    -n "$n" \
    -byGroupByDate "$dateRegex" \
    -dateIndex "$dateIndex"


hadoop jar ExtractNgrams.jar org.wwbp.ExtractNgrams \
    -libjars "$libjars" \
    -input "$input" \
    -output "$output" \
    -message_field "$message_field" \
    -group_id_index "$group_id" \
    -n "$n" \
    -byGroupByDate "$dateRegex" \
    -dateIndex "$dateIndex"
