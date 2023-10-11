# ~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/anscombe_transform.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id

from math import sqrt
import sys, os, argparse

from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F

DEF_INPUTFILE = ["/hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id"] # supports multiple files

DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__)) # '/hadoop-tools/sparkScripts'

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Group norms are re-based on Anscombe'd values")
    parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
                help='')
    args = parser.parse_args()

    if DEF_INPUTFILE is None:
        print("You must specify --input_file")
        sys.exit()

    input_file, header = args.input_file, False

    ## Spark Portion:
    session = SparkSession.builder\
            .config("spark.driver.memory","8g")\
            .appName("anscombeTransform")\
            .getOrCreate()
    sc = session.sparkContext

    # Read in tweets and display a sample
    msgsDF = session.read.csv(input_file, header=header)

    # label fields
    group_field = "_c0"   
    feat_field = "_c1"
    count_field = "_c2"
    freq_field = "_c3"
    count_field_ans = "count_anscombe"
    freq_field_ans = "group_norm_anscombe"

    # 2020 ONLY, remove bad data
    if "2020" in input_file:
        print("Running extra clean up for 2020 data\n")
        msgsDF = msgsDF.filter( (msgsDF[group_field] >= "2020_01") & (msgsDF[group_field]  < "2021_01") )
        #msgsDF = msgsDF.dropDuplicates()

    if "2019" in input_file:
        print("Running extra clean up for 2019 data\n")
        msgsDF = msgsDF.filter( (msgsDF[group_field] >= "2019_01") & (msgsDF[group_field]  < "2020_01") )

    print("Removing user mentions from ngrams")
    msgsDF = msgsDF.filter( ~msgsDF[feat_field].startswith("@") )

    print("Removing punctuation from ngrams")
    msgsDF = msgsDF.withColumn(feat_field, F.regexp_replace(F.col(feat_field), '[^\w\s]', ''))
    
    print("Removing blank ngrams")
    msgsDF = msgsDF.filter(msgsDF[feat_field] != "")

    print("Usable ngram Data",input_file)
    msgsDF.show(n=20,truncate=False)
    #print("Original Data Length:",msgsDF.count())

    # transformation UDFs
    def anscombe_transform(num):
        try:
            float_num = float(num.strip().strip('"').strip("'"))
        except:
            return float(0.0)
        return 2*sqrt(float_num+3/float(8))
    anscombe_transform_udf = udf(anscombe_transform, FloatType())

    # anscombe transformed counts
    print("Anscombe transform the counts")
    msgsDF = msgsDF.withColumn(count_field_ans, anscombe_transform_udf(count_field))

    # Re-base group norms
    print("Re-basing group norms on new counts")
    w = Window.partitionBy(group_field)
    msgsDF = msgsDF.withColumn(freq_field_ans, F.col(count_field_ans) / F.sum(col(count_field_ans)).over(w))

    print("Anscombe Transformed Data")
    msgsDF.show(20,True)

    # print("STOPPING EARLY")
    # sc.stop()
    # sys.exit(0)
    
    # Drop unused columns
    msgsDF = msgsDF.drop(freq_field).drop(count_field_ans)

    # write to HDFS
    output_file = input_file.replace('feat.','featANS.') 
    print("Writing to",output_file)
    msgsDF.coalesce(1).write.csv(output_file, quoteAll=False) 

    # Close spark context
    sc.stop()