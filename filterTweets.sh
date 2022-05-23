#!/bin/bash

# Check params
if [ $# -ne 3 ]
then
    echo "./filterTweets.sh input message_field group_field"
    exit 1
fi

# Collect pararms
input="$1"
message_field="$2"
group_field="$3"
output="${input}_english"
langs="en"

if hadoop fs -test -d "$output"
then
    echo "Already a directory: $output"
    exit
fi

# --- english filtering ---
cd hadoop-tools/languageFilter/
# Getting working directory
WD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
echo "Running Language Filter Test"
source "$WD/classpath.sh"
cd "$WD/classes"
javac -d . ../LanguageFilter.java || exit
rm LanguageFilter.jar && jar -cvf LanguageFilter.jar -C . .
echo "----------------------"
hadoop jar LanguageFilter.jar org.wwbp.languageFilter.LanguageFilter \
    -libjars "$libjars" \
    -input "$input" \
    -output "$output" \
    -message_field "$message_field" \
    -languages "$langs"
# return to home dir
cd "$WD"
cd ../..

# --- all other filtering ---
input=$output
output="${output}_deduped"
~/spark/bin/spark-submit /hadoop-tools/sparkScripts/deduplicate_and_filter.py --input_file "$input" --output_file "$output" --message_field "$message_field" --group_field "$group_field"