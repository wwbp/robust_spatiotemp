# ~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/tweets_to_upt.py --input_file /hadoop_data/ctlb/2020/dedup/timelines2020*.csv --output_file /hadoop_data/ctlb/2020/feats/timelines2020_en_dedup_full.csv --date_field 3 --group_field 1

#!/usr/bin/env python
import numpy as np
import io, csv, re
import sys, os, argparse

from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, countDistinct

DEF_INPUTFILE = None
DEF_OUTPUTFILE = None
DEF_DATE_FIELD = 2
DEF_GROUP_FIELD = 1
DEF_ANNON = False

DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__)) # '/hadoop-tools/sparkScripts'

datetime_format = "%Y-%m-%d %H:%M:%S"

## Spark Portion:

# get yearweek_userid from datetime and userid
def make_yearweek_userid(date, date2, user):
    if user is None: return "" 
    try:
        if date is None: return ":" + str(user) 
        d = datetime.strptime(date, datetime_format)
        yw = d.strftime('%Y_%V')
        return yw + ":" + str(user)
    except ValueError:
        pass
    try: # try date2 if date is invalid
        if date2 is None: return ":" + str(user) 
        d = datetime.strptime(date2, datetime_format)
        yw = d.strftime('%Y_%V')
        return yw + ":" + str(user)
    except ValueError:
        return ":" + str(user)
    

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Remove duplicate tweets within a group")
    parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
                help='')
    parser.add_argument('--output','--output_file', dest='output_file', default=DEF_OUTPUTFILE,
                help='')
    parser.add_argument('--date', '--date_field', dest='date_field', type=int, default=DEF_DATE_FIELD,
                help='')
    parser.add_argument('--group', '--group_field', dest='group_field', type=int, default=DEF_GROUP_FIELD,
                help='')
    parser.add_argument('--no_anonymize', action='store_false', dest='anonymize', default=DEF_ANNON,
                help='')
    args = parser.parse_args()

    if not (args.input_file and args.output_file):
        print("You must specify --input_file and --output_file")
        sys.exit()

    input_file, output_file, date_field, group_field, anonymize, header = args.input_file, args.output_file, args.date_field, args.group_field, args.anonymize, False

    session = SparkSession.builder\
            .config("spark.driver.memory","8g")\
            .appName("countUPTs")\
            .getOrCreate()
    sc = session.sparkContext

    # Read in tweets and display a sample
    msgsDF = session.read.csv(input_file, header=header)
    print("Original Data")
    msgsDF.sample(False, 0.01, seed=0).show(n=10,truncate=False)
    #print("Raw Data Length:",msgsDF.count())

    yw_ui_field = "yearweek_userid"
    if not header:
        date_field_num = date_field
        dup_field = 'temp_column'
        date_field = "_c" + str(date_field_num)
        date_field_2 = "_c" + str(date_field_num + 1)
        group_field = "_c" + str(group_field)

    # Make yearweek_userid column and display a sample
    yearweek_userid_udf = udf(make_yearweek_userid, StringType())
    msgsDF = msgsDF.withColumn(yw_ui_field, yearweek_userid_udf(date_field, date_field_2, group_field)).drop("_c6").drop("_c7")

    # View processed data
    print("Data with YearWeek:UserId")
    msgsDF.sample(False, 0.01, seed=0).limit(10).show()

    # group by yearweek_userid --> | yearweek_userid | count |
    uptCountDF = msgsDF.groupBy(yw_ui_field).count()

    # Debug print outs
    # print("User Post Totals")
    # uptCountDF.show(10,False)
    # print("num unique yearweek_userids:",uptCountDF.count())
    # print("Unique Counts (User ID)")
    # msgsDF.select(countDistinct("_c1")).show()
    # print("Unique Counts (Message ID)")
    # msgsDF.select(countDistinct("_c0")).show()    

    # print("STOPPING EARLY")
    # sc.stop()
    # sys.exit()

    # filter msgsDF on the yearweek_userids in uptCountDF using inner join

    # print("User Post Totals (at least 1)")
    # msgsDF.select([yw_ui_field]+list(msgsDF.columns)[:-1]).write.csv( output_file.replace(".csv","_1upts.csv") ) # must re-order table before saving

    # print("User Post Totals (at least 2)")
    # uptCountDF = uptCountDF.where(col("count") >= 2)
    # print("num valid yearweek_userids",uptCountDF.count()) # num unique users
    # msgsDF.join(uptCountDF,yw_ui_field,"inner").drop(col("count")).write.csv( output_file.replace(".csv","_2upts.csv") )

    print("User Post Totals (at least 3)")
    uptCountDF = uptCountDF.where(col("count") >= 3)
    print("num valid yearweek_userids",uptCountDF.count()) 
    msgsDF.join(uptCountDF,yw_ui_field,"inner").drop(col("count")).write.csv( output_file.replace(".csv","_3upts.csv") )
    
    # print("User Post Totals (at least 5)")
    # uptCountDF = uptCountDF.where(col("count") >= 5)
    # print("num valid yearweek_userids",uptCountDF.count()) # num unique users
    # msgsDF.join(uptCountDF,yw_ui_field,"inner").drop(col("count")).write.csv( output_file.replace(".csv","_5upts.csv") )

    # Close spark context
    sc.stop()