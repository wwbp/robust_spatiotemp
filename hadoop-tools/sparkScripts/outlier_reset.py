'''
Use as:
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty --no_sigma
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty --no_scale
'''

#!/usr/bin/env python
import sys, os, argparse

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import udf, col
from pyspark.sql import Window
from pyspark.sql import functions as F

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# How many sigma is considered an outlier
sigma_removal = True
sigma = 3

# What are the scaling bounds?
rescaling = True
min_val, max_val = 0, 5

DEF_INPUTFILE = None
DEF_OUTPUTFILE = None
DEF_DATE_FIELD = 2
DEF_GROUP_FIELD = 1
DEF_ANNON = False



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

# get yearweek from yearweek_userid
def make_yw(ywui):
    if ywui is None: return ""
    try:
        yw, _ = ywui.split(":")
        return yw
    except ValueError:
        return ""

def clean_num_feats(num):
    try:
        float_num = float(num.strip().strip('"').strip("'"))
    except:
        return float(0.0)
    return float_num

## Spark Portion:

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Remove outlier results within a group")
    parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
                help='')
    parser.add_argument('--no_sigma', dest='sigma', action='store_false')
    parser.add_argument('--no_scale', dest='scale', action='store_false')
    args = parser.parse_args()

    if not (args.input_file):
        print("You must specify --input_file")
        sys.exit()

    input_file, header, sigma_removal, rescaling = args.input_file, args.sigma, args.scale, False
    output_file = input_file

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
    user_field = "userid"
    yw_field = "yw"
    msgsDF = msgsDF.withColumnRenamed("_c0",yearweek_userid_field)
    msgsDF = msgsDF.withColumnRenamed("_c1",feat_field)
    msgsDF = msgsDF.withColumnRenamed("_c2",field_count_field)
    msgsDF = msgsDF.withColumnRenamed("_c3",field_freq_field)

    print("Original Data")
    msgsDF.show(20,False)

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

    # create userid field
    userid_county_udf = udf(make_userid, StringType())
    msgsDF = msgsDF.withColumn(user_field, userid_county_udf(yearweek_userid_field))

    # create yearweek field
    yw_county_udf = udf(make_yw, StringType())
    msgsDF = msgsDF.withColumn(yw_field, yw_county_udf(yearweek_userid_field))
    msgsDF = msgsDF.filter(col(yw_field) != "")

    print("Cleaned Data")
    msgsDF.show(20,False)

    print("Length of Original Data:", msgsDF.count())

    # remove anything above and below sigma
    if sigma_removal:
        meanCol = 'mean_freq'
        stddevCol = 'std_freq'
        numeric_cols = [field_freq_field]
        mean_std = \
            msgsDF \
            .groupBy(yw_field, feat_field) \
            .agg( F.mean(field_freq_field).alias(meanCol), F.stddev_pop(col(field_freq_field).cast("double")).alias(stddevCol) )
        minCol = 'min_freq'
        maxCol = 'max_freq'
        mean_std = mean_std.withColumn(minCol, F.col(meanCol) - (sigma * F.col(stddevCol)) )
        mean_std = mean_std.withColumn(maxCol, F.col(meanCol) + (sigma * F.col(stddevCol)) )

        print("Upper and Lower Bounds")
        mean_std.show(20,False)

        # Replace Outliers (n sigma away) with the mean
        msgsDF = msgsDF.join(mean_std, how = 'left', on = [yw_field, feat_field])
        mean_std.unpersist(blocking=True)

        # Set outliers to mean for yearweek
        msgsDF = msgsDF.withColumn(field_freq_field, F.when((F.col(field_freq_field) > F.col(maxCol)) | (F.col(field_freq_field) < F.col(minCol)), F.col(meanCol)).otherwise(F.col(field_freq_field)))    
        print("Outliers Identifed, there are:", msgsDF.where(msgsDF[field_freq_field] == msgsDF[meanCol]).count()) 

    # rescale feat_freq between two numbers
    if rescaling:
        # before rescaling
        # msgsDF.agg({field_freq_field: 'min'}).show()
        # msgsDF.agg({field_freq_field: 'max'}).show()

        # clip results
        # msgsDF = msgsDF.withColumn(field_freq_field, F.when(F.col(field_freq_field) > max_val, max_val).otherwise(F.col(field_freq_field)))
        # msgsDF = msgsDF.withColumn(field_freq_field, F.when(F.col(field_freq_field) < min_val, min_val).otherwise(F.col(field_freq_field)))

        # UDF for converting column type from vector to double type
        unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())
        # VectorAssembler Transformation - Converting column to vector type
        assembler = VectorAssembler(inputCols=[field_freq_field],outputCol=field_freq_field+"_Vect")
        scaler = MinMaxScaler(min=min_val, max=max_val, inputCol=field_freq_field+"_Vect", outputCol=field_freq_field+"_Scaled")
        pipeline = Pipeline(stages=[assembler, scaler])
        # Fitting pipeline on dataframe
        msgsDF = pipeline.fit(msgsDF).transform(msgsDF).withColumn(field_freq_field, \
            unlist(field_freq_field+"_Scaled")).drop(field_freq_field+"_Vect").drop(field_freq_field+"_Scaled")

        # confirm rescaling
        # msgsDF.agg({field_freq_field: 'min'}).show()
        # msgsDF.agg({field_freq_field: 'max'}).show()
       
    print("Processed messages")
    msgsDF.show(20,False)

    msgsDF = msgsDF.select(col(yearweek_userid_field),col(feat_field),col(field_count_field),col(field_freq_field))

    if rescaling:
        output_file = output_file.replace("weighted","{}{}scale".format(min_val,max_val))
    else:
        output_file = output_file.replace("weighted","notscaled")

    if sigma_removal:
        output_file = output_file.replace("upts","upts{}sig".format(sigma))
    
    # Write to file
    print("Writing data to output file:", output_file)
    msgsDF.write.csv(output_file, quoteAll=True)

    # Close spark context
    sc.stop()
