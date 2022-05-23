#!/bin/bash

# Getting working directory
WD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo "Running Language Filter Test"
# ./runThisLocally.sh "input" "output" "message_field"
input="$1"
output="$2"
message_field="$3"
source "$WD/classpath.sh"

cd "$WD/classes"
javac -d . ../LanguageFilter.java || exit
rm LanguageFilter.jar && jar -cvf LanguageFilter.jar -C . .
echo "----------------------"
java org.wwbp.languageFilter.LanguageFilter \
-libjars "$libjars" \
-input "$input" \
-output "$output" \
-message_field "$message_field" \
-testing

