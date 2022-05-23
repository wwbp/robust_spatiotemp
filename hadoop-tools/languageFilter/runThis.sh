#!/bin/bash

# Getting working directory
WD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Running Language Filter Test"
# ./runThisLocally.sh "input" "output" "message_field"

if [ $# -ne 4 ]; then
    echo "./runThis.sh input output message_field lang1,lang2,..."
    exit
fi

input="$1"
output="$2"
message_field="$3"
langs="$4"

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
