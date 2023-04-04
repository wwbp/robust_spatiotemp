'''
Use as:
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_lex_3upts.yw_user_id.cnty.wt.05fc --output_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_lex_3upts.yw_cnty.wt.05fc 
'''

#!/usr/bin/env python
import numpy as np
import io, csv, re
import sys, os, argparse

import pandas as pd

from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import udf, col, avg, countDistinct, sum, stddev_pop
import pyspark.sql.functions as F
from pyspark.sql import Window

MIN_USERS_IN_GROUP = 50 # TODO Set GFT 5, 30, 80, 100, 200, 300, 500

DEF_INPUTFILE =  "/hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_lex_3upts.yw_user_id.cnty.wt"
DEF_OUTPUTFILE = "/hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_lex_3upts.yw_cnty"
DEF_MIN_USERS_IN_GROUP = 50 # Set GFT 50, 200, 500

DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__)) # '/hadoop-tools/sparkScripts'

# get yearweek_county from yearweek_userid and county
def make_yearweek_county(ywui, cnty):
    if ywui is None or cnty is None: return ""
    try:
        yw, ui = ywui.split(":")
        return yw + ":" + str(cnty)
    except ValueError:
        return ""
def make_yearweek_supercounty(ywui, supercnty):
    if ywui is None or supercnty is None: return ""
    try:
        yw, ui = ywui.split(":")
        return yw + ":" + str(supercnty)
    except ValueError:
        return ""

# get yearweek_county from yearweek_userid and county
def make_yearmonth_county(ywui, cnty):
    if ywui is None or cnty is None: return ""
    try:
        yw, ui = ywui.split(":")
        date = datetime.strptime(yw + '-1', "%Y_%W-%w")
        ym = date.strftime("%Y_%m")
        return ym + ":" + str(cnty)
    except ValueError:
        return ""

# get yearweek_county from yearweek_userid and county
def make_year_county(ywui, cnty):
    if ywui is None or cnty is None: return ""
    try:
        yw, ui = ywui.split(":")
        year, week = yw.split("_")
        return year + ":" + str(cnty)
    except ValueError:
        return ""

# get userid from yearweek_userid
def make_userid(ywui):
    if ywui is None: return ""
    try:
        _, ui = ywui.split(":")
        return ui
    except ValueError:
        return ""

# get yw from yearweek_cnty
def make_yearweek(ywui):
    if ywui is None: return ""
    try:
        yw, cnty = ywui.split(":")
        return yw
    except ValueError:
        return ""

def clean_num_feats(num):
    if isinstance(num, float):
        return num
    try:
        float_num = float(num.strip().strip('"').strip("'"))
    except:
        return float(0.0)
    return float_num

## Spark Portion:

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Remove duplicate tweets within a group")
    parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
                help='')
    parser.add_argument('--output','--output_file', dest='output_file', default=DEF_OUTPUTFILE,
                help='')
    parser.add_argument('--gft','--n', dest='gft', default=DEF_MIN_USERS_IN_GROUP,
                help='')
    args = parser.parse_args()

    if not (args.input_file and args.output_file):
        print("You must specify --input_file and --output_file")
        sys.exit()

    input_file, output_file, MIN_USERS_IN_GROUP, header = args.input_file, args.output_file, args.gft, False

    session = SparkSession\
            .builder\
            .appName("aggFeatsByGroup")\
            .getOrCreate()
    sc = session.sparkContext

    if not header:
        dup_field = 'temp_column'


    # Read in tweets and display a sample
    msgsDF = session.read.csv(input_file, header=header)
    msgsDF = msgsDF.na.drop()

    # rename columns
    yearweek_userid_field = "yearweek_userid"
    feat_field = "feat"
    field_count_field = "feat_count"
    field_freq_field = "feat_freq"
    county_field = "cnty"
    yearweek_field = "yw"
    supercnty_field = "supercnty"
    supercounty_field = "supercnty"
    supercounty_weight_field = "supercnty_wt"
    user_field = "userid"
    yearweek_userid_weight = "ywui_wt"
    yw_cnty_field = "yearweek_cnty"
    ym_cnty_field = "yearmonth_cnty"
    yw_supercnty_field = "yearweek_supercnty"
    year_cnty_field = "year_cnty"
    group_field = yw_supercnty_field # TODO yw_cnty_field / yw_supercnty_field / user_field / county_field / ym_cnty_field / yearweek_field
    msgsDF = msgsDF.withColumnRenamed("_c0",yearweek_userid_field)
    msgsDF = msgsDF.withColumnRenamed("_c1",feat_field)
    msgsDF = msgsDF.withColumnRenamed("_c2",field_count_field)
    msgsDF = msgsDF.withColumnRenamed("_c3",field_freq_field)
    msgsDF = msgsDF.withColumnRenamed("_c4",county_field)
    if '_c5' in msgsDF.columns:
        msgsDF = msgsDF.withColumnRenamed("_c5",yearweek_userid_weight)
    if '_c6' in msgsDF.columns:
        msgsDF = msgsDF.withColumnRenamed("_c6",yearweek_field)

    print("Original Data -",input_file)
    msgsDF.sample(False, 0.01, seed=1).limit(10).show(10,False)

    # Clean columns
    clean_ywui = udf(lambda x:x.strip().replace("'","").replace('"',""), StringType())
    msgsDF = msgsDF.withColumn(yearweek_userid_field, clean_ywui(yearweek_userid_field))

    clean_text_feats = udf(lambda x:x.strip().strip("'").strip('"'), StringType())
    msgsDF = msgsDF.withColumn(feat_field, clean_text_feats(feat_field))
    msgsDF = msgsDF.filter(col(feat_field) != "")
    msgsDF = msgsDF.filter(~col(feat_field).startswith("@"))

    clean_num_feats_udf = udf(clean_num_feats, FloatType())
    msgsDF = msgsDF.withColumn(field_count_field, clean_num_feats_udf(field_count_field))
    msgsDF = msgsDF.filter(col(field_count_field) > 0.0)
    msgsDF = msgsDF.withColumn(field_freq_field, clean_num_feats_udf(field_freq_field))

    clean_cnty = udf(lambda x:x.strip().replace("'","").replace('"',"").zfill(5), StringType())
    msgsDF = msgsDF.withColumn(county_field, clean_cnty(county_field))

    # create yearweek_county field
    yearweek_county_udf = udf(make_yearweek_county, StringType())
    msgsDF = msgsDF.withColumn(yw_cnty_field, yearweek_county_udf(yearweek_userid_field, county_field))

    # create yearmonth_county field
    yearmonth_county_udf = udf(make_yearmonth_county, StringType())
    msgsDF = msgsDF.withColumn(ym_cnty_field, yearmonth_county_udf(yearweek_userid_field, county_field))

    # create year_county field
    year_county_udf = udf(make_year_county, StringType())
    msgsDF = msgsDF.withColumn(year_cnty_field, year_county_udf(yearweek_userid_field, county_field))

    # create userid field
    make_userid_udf = udf(make_userid, StringType())
    make_yearweek_udf = udf(make_yearweek, StringType())
    msgsDF = msgsDF.withColumn(user_field, make_userid_udf(yearweek_userid_field))

    # if ywui_wt exists then prepare for use
    if yearweek_userid_weight in msgsDF.columns:
        msgsDF = msgsDF.withColumn(yearweek_userid_weight, clean_num_feats_udf(yearweek_userid_weight))
        # msgsDF = msgsDF.withColumn(field_freq_field, col(field_freq_field) * col(yearweek_userid_weight)) # pre-multiply weights

    print("Cleaned Data")
    msgsDF.show(20,False)

    if "super" in group_field: # only calculate for super counties

        # Add on the super counties and super county weights
        mapping_file = "/home/smangalik/hadoop-tools/sparkScripts/cnty_supes_mapping.csv"
        cnty_supes_df = pd.read_csv(mapping_file, dtype={'cnty':'str', 'cnty_w_sups300':'str'})
        cnty_supes_df = cnty_supes_df.dropna(how='any',axis=0) # remove missing mappings
        cnty_supes_mapping = dict(zip(cnty_supes_df['cnty'], cnty_supes_df['cnty_w_sups300']))
        supes_wt_mapping = dict(zip(cnty_supes_df['cnty'], cnty_supes_df['weight']))
        super_county_udf = udf(lambda x: str(cnty_supes_mapping.get(x,"")), StringType())
        super_county_weight_udf = udf(lambda x: float(supes_wt_mapping.get(x,0.0)), FloatType())
        msgsDF = msgsDF.withColumn(supercounty_field, super_county_udf(county_field))
        msgsDF = msgsDF.withColumn(supercounty_weight_field, super_county_weight_udf(county_field))
        # Remove any unreliable counties
        msgsDF = msgsDF.filter(msgsDF[supercounty_weight_field] > 0.0)

        # Create yearweek_supercounty field
        yearweek_supercounty_udf = udf(make_yearweek_supercounty, StringType())
        msgsDF = msgsDF.withColumn(yw_supercnty_field, yearweek_county_udf(yearweek_userid_field, supercounty_field))
        # Split data by super county or not
        msgsDF_super = msgsDF.filter(msgsDF[supercounty_weight_field] != 1.0)
        msgsDF = msgsDF.filter(msgsDF[supercounty_weight_field] == 1.0)
        msgsDF = msgsDF.withColumn("weight_sum", sum(supercounty_weight_field).over(Window.partitionBy([col(group_field), col(feat_field)])))
        # mutliply counts and freqs by weights
        msgsDF_super = msgsDF_super.withColumn(field_count_field, col(field_count_field) * col(supercounty_weight_field))
        msgsDF_super = msgsDF_super.withColumn(field_freq_field, col(field_freq_field) * col(supercounty_weight_field))
        # sum up the counts/weights per yearweek_supercounty+feat
        msgsDF_super = msgsDF_super.withColumn(field_count_field, sum(field_count_field).over(Window.partitionBy([col(group_field), col(feat_field)])))
        msgsDF_super = msgsDF_super.withColumn(field_freq_field, sum(field_freq_field).over(Window.partitionBy([col(group_field), col(feat_field)])))
        # sum all the weights for each yearweek_supercounty+feat, then divide all values by the sum
        msgsDF_super = msgsDF_super.withColumn("weight_sum", sum(supercounty_weight_field).over(Window.partitionBy([col(group_field), col(feat_field)])))
        msgsDF_super = msgsDF_super.withColumn(field_count_field, col(field_count_field) / col("weight_sum"))
        msgsDF_super = msgsDF_super.withColumn(field_freq_field, col(field_freq_field) / col("weight_sum"))
        print("Cleaned Data (Supers Only)")
        msgsDF_super.sample(withReplacement=False, fraction=0.001).show(20,False)
        
        # stack the two dataframes on top of each other!
        msgsDF = msgsDF.union(msgsDF_super)
        msgsDF_super.unpersist(blocking = True)

        print("Cleaned Data (Corrected by Weight)")
        msgsDF.sample(withReplacement=False, fraction=0.001).show(20,False)

    # Filter with broadcasted list
    # valid_entities = list(userCountDF.select(group_field).toPandas()[group_field])
    # valid_entities_bc = sc.broadcast(valid_entities)
    # userCountDF.unpersist(blocking = True)
    # msgsDF = msgsDF.where( (msgsDF[group_field].isin(valid_entities_bc.value)) )
    # valid_entities_bc.destroy()

    # Filter with an inner join
    # msgsDF = msgsDF.join(userCountDF,group_field,"inner").drop(col("distinct_users"))

    # Get count of unique values
    # print("Unique Yearweek UserIDs")
    # msgsDF.select(countDistinct(yearweek_userid_field)).show(20,False)
    # print("Unique Entities")
    # msgsDF.select(countDistinct(group_field)).show(20,False)
    # print("Unique Counties")
    # msgsDF.select(countDistinct(county_field)).show(20,False)

    # Group By group field and feature
    print("Grouping by feature and",group_field)
    aggDF = msgsDF.groupBy([group_field,feat_field]).agg(
        sum(field_count_field).alias('sum_N_lexwords'), 
        (F.sum(col(field_freq_field) * col(yearweek_userid_weight))/F.sum(col(yearweek_userid_weight))).alias("wavg_score"),
        # TODO (sum[weights*(score - weighted_avg_score)**2]/sum(weights))**0.5
        #((F.sum(col(yearweek_userid_weight)*(col(field_freq_field) - col("avg_score_wt"))**2)/F.sum(col(yearweek_userid_weight)))**0.5).alias('std_score_wt'),
        avg(field_freq_field).alias('avg_score'),
        stddev_pop(field_freq_field).alias('std_score'),
        countDistinct(user_field).alias('N_users')
    )
    # Add time and space column if group_field is splittable
    if group_field != yearweek_field:
        aggDF = aggDF.withColumn("Space", make_userid_udf(group_field))
        aggDF = aggDF.withColumn("Time", make_yearweek_udf(group_field))
    aggDF.sample(False, 0.1, seed=1).limit(10).show(20,False)
    #aggDF.toPandas().to_csv('~/yearweek_user_counts_2019.csv', index=False) # write to local before GFT

    # GFT filter to MIN_USERS_IN_GROUP
    #aggDF = aggDF.where(col("N_users") >= MIN_USERS_IN_GROUP) 
    #print("Aggregated Data with over",MIN_USERS_IN_GROUP,"users per",group_field)
    #aggDF.sample(False, 0.01, seed=1).limit(10).show(20,False)

    # print('STOPPING EARLY')
    # sc.stop()
    # sys.exit()

    # Write to file
    print("Writing data to output file",output_file)
    #aggDF.write.csv(output_file.replace("upts","upts_{}users".format(MIN_USERS_IN_GROUP)), quoteAll=True)
    aggDF.write.csv(output_file, quoteAll=False)

    # Close spark context
    sc.stop()
