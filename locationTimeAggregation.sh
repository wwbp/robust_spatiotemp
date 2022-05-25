#!/bin/bash

# Check params
if [ $# -ne 5 ]
then
    echo "./locationTimeAggregation.sh HDFS_INPUT_CSV GROUP_ID_IDX AGG_CSV MIN_GROUPS [HDFS_OUTPUT_CSV]"
    exit 1
fi

# Collect pararms
input="$1"
group="$2"
agg_mapping="$3"
gft="$4"
output="$5"



if hadoop fs -test -d "$output"
then
    echo "Already a directory: $output"
    exit
fi

# add higher level mapping
~/spark/bin/spark-submit /hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/add_county_to_feat_tables-ctlb.py --word_table "$input" --mapping_file "$agg_mapping"

# merge iterations
hadoop fs -cat "${input}.cnty_*/*" | hadoop fs -put - "${input}.cnty"
# delete iterations
hadoop fs -rm -r "${input}.cnty_*"

# agg results
input="${input}.cnty"
~/spark/bin/spark-submit /hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file "$input" --output_file "$output" --gft "$gft" --group "$group"