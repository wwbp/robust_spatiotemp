#!/bin/bash

# Check params
if [ $# -ne 5 ]
then
    echo "./lexiconExtractAndScoresh INPUT MESSAGE_FIELD GROUP_ID WEIGHTS LEXICON"
    exit 1
fi

# Collect pararms
input="$1"
base = "basename ${input}"
dir = "dirname ${input}"
output="${dir}/feat.1gram.${base}"
message_field="$2"
group_id="$3"
weight_mapping="$4"
lexicon="$5"

if hadoop fs -test -d "$output"
then
    echo "Already a directory: $output"
    exit
fi


# --- 1gram extraction ---
cd hadoop-tools/nGramExtraction/
# Getting working directory
WD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source "$WD/classpath.sh"
cd "$WD/classes"
# Execute jar
javac -d . ../ExtractNgrams.java || exit
rm ExtractNgrams.jar && jar -cvf ExtractNgrams.jar -C . .
echo hadoop jar ExtractNgrams.jar org.wwbp.ExtractNgrams -libjars "$libjars" -input "$input"  -output "$output" -message_field "$message_field" -group_id_index "$group_id" -n "$n"
hadoop jar ExtractNgrams.jar org.wwbp.ExtractNgrams \
    -libjars "$libjars" \
    -input "$input" \
    -output "$output" \
    -message_field "$message_field" \
    -group_id_index "$group_id" \
    -n "1"
# return to home dir
cd "$WD"
cd ../..


# --- reweight users ---
input=$output
output="${output}_weighted"
# generate weighted iterations
~/spark/bin/spark-submit /hadoop-tools/sparkScripts/reweight_userid_feats.py --input "$input" --output "$output" --mapping_file "$weight_mapping"
# merge iterations
hadoop fs -cat "${output}_*/*" | hadoop fs -put - "$output"
# delete iterations
hadoop fs -rm -r "${output}_*"


# --- reset outlier word usage ---
input=$output
replace="upts3sig"
output=${input/upts/$replace} 
~/spark/bin/spark-submit /hadoop-tools/sparkScripts/outlier_reset.py --input_file "$input" --no_scale


# --- generate DEP/ANX scores---
input=$output
replace="basename ${lexicon} .csv"
output = ${input/1gram/$replace} 
~/spark/bin/spark-submit /hadoop-tools/sparkScripts/topics_extraction.py --lex_file "$lexicon" --word_table "$input" --output_file "$output"


# --- rescale scores---
input=$output
~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file "$input" --no_sigma