#!/bin/bash

# Check params
if [ $# -ne 4 ]
then
    echo "./locationTimeAggregation.sh INPUT OUTPUT GFT GROUP_BY"
    exit 1
fi

# Collect pararms
input="$1"
output="$2"
gft="$3"
group="$4"

if hadoop fs -test -d "$output"
then
    echo "Already a directory: $output"
    exit
fi

~/spark/bin/spark-submit /hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file "$input" --output_file "$output" --gft "$gft" --group "$group"