'''
Use as:

~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/scale_outlier.py --input_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_lex_3upts.yw_user_id.cnty.wt --no_sigma

~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/scale_outlier.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/scale_outlier.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty --no_scale
'''

#!/usr/bin/env python
import sys, os, argparse

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField
from pyspark.sql.functions import udf, col, mean, stddev_pop, greatest, least, rank
from pyspark.sql import Window
from pyspark.sql import functions as F


# How many sigma is considered an outlier
sigma_removal = False
sigma = 3

# What are the scaling bounds?
rescaling = True
floor, ceiling = 0, 5
standardize = False
mean_center = False 
post_standardize_fc = False

DEF_INPUTFILE = None


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

    input_file, header, sigma_removal, rescaling = args.input_file, False, args.sigma, args.scale
    output_file = input_file

    session = SparkSession\
            .builder\
            .appName("aggFeatsByGroup")\
            .getOrCreate()
    sc = session.sparkContext

    if not header:
        dup_field = 'temp_column'

    # schema = StructType([ \
    #     StructField(yearweek_userid_field,StringType(),False), \
    #     StructField(feat_field,StringType(),False), \
    #     StructField(field_count_field,FloatType(),True), \
    #     StructField(field_freq_field, FloatType(), True), \
    #     StructField(county_field, StringType(), False), \
    #     StructField(weight_field, FloatType(), True) \
    # ])

    # Read in tweets and display a sample
    msgsDF = session.read.csv(input_file, header=header)#, schema=schema)
    msgsDF = msgsDF.na.drop()

    # rename columns
    yearweek_userid_field = "yearweek_userid"
    feat_field = "feat"
    field_count_field = "feat_count"
    field_freq_field = "feat_freq"
    weight_field = "weight"
    user_field = "userid"
    county_field = "cnty"
    yw_field = "yw"
    msgsDF = msgsDF.withColumnRenamed("_c0",yearweek_userid_field)
    msgsDF = msgsDF.withColumnRenamed("_c1",feat_field)
    msgsDF = msgsDF.withColumnRenamed("_c2",field_count_field)
    msgsDF = msgsDF.withColumnRenamed("_c3",field_freq_field)
    msgsDF = msgsDF.withColumnRenamed("_c4",county_field)
    msgsDF = msgsDF.withColumnRenamed("_c5",weight_field)    

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
    sigma_removal = False # remove if you somehow want to turn this back on
    if sigma_removal:
        print("Sigma based filtering")
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
        print("Rescaling")

        # Quick data stats
        # msgsDF.agg({field_freq_field: 'min'}).show()
        # msgsDF.agg({field_freq_field: 'max'}).show()
        #print("Length of Data < 0.0:", msgsDF.where(col(field_freq_field) < floor).count())
        #print("Length of Data > 5.0:", msgsDF.where(col(field_freq_field) > ceiling).count())

        # score = ceil(floor(score, 0), 5) 
        def thresh_score(score): 
            return min(max(float(score),0.0),5.0)
        print("Applying floor of {} and ceiling of {}".format(floor,ceiling))  
        thresh_udf = udf(thresh_score, FloatType())
        msgsDF = msgsDF.withColumn(field_freq_field, thresh_udf(field_freq_field))#, feat_field))

        msgsDF_dep = msgsDF.where(col(feat_field)=="DEP_SCORE")
        msgsDF_anx = msgsDF.where(col(feat_field)=="ANX_SCORE")
        msgsDF_ang = msgsDF.where(col(feat_field)=="ANG_SCORE")

        # low_dep_users = list(msgsDF_dep.where(col(field_freq_field) == floor).select(user_field).distinct().toPandas()[user_field])[:100]
        # high_dep_users = list(msgsDF_dep.where(col(field_freq_field) == ceiling).select(user_field).distinct().toPandas()[user_field])[:100]
        # low_anx_users = list(msgsDF_anx.where(col(field_freq_field) == floor).select(user_field).distinct().toPandas()[user_field])[:100]
        # high_anx_users = list(msgsDF_anx.where(col(field_freq_field) == ceiling).select(user_field).distinct().toPandas()[user_field])[:100]
        # print("Low DEP Users",low_dep_users)
        # print("High DEP Users",high_dep_users)
        # print("Low ANX Users",low_anx_users)
        # print("High ANX Users",high_anx_users)
        

        # print("STOPPING EARLY")
        # sc.stop()
        # sys.exit(0)

        msgsDF_dep_stats = msgsDF_dep.select(
            mean(col(field_freq_field)).alias('mean'),
            stddev_pop(col(field_freq_field)).alias('std')
        ).collect()

        msgsDF_anx_stats = msgsDF_anx.select(
            mean(col(field_freq_field)).alias('mean'),
            stddev_pop(col(field_freq_field)).alias('std')
        ).collect()

        msgsDF_ang_stats = msgsDF_ang.select(
            mean(col(field_freq_field)).alias('mean'),
            stddev_pop(col(field_freq_field)).alias('std')
        ).collect()

        mean_dep = msgsDF_dep_stats[0]['mean']
        std_dep = msgsDF_dep_stats[0]['std']
        mean_anx = msgsDF_anx_stats[0]['mean']
        std_anx = msgsDF_anx_stats[0]['std']
        mean_ang = msgsDF_ang_stats[0]['mean']
        std_ang = msgsDF_ang_stats[0]['std']

        print("Mean DEP_SCORE: {} (std {})".format(mean_dep,std_dep))
        print("Mean ANX_SCORE: {} (std {})".format(mean_anx,std_anx))
        print("Mean ANG_SCORE: {} (std {})".format(mean_ang,std_ang))

        if standardize:
            print("Z-scoring all scores")
            msgsDF_dep = msgsDF_dep.withColumn(field_freq_field, ((F.col(field_freq_field) - mean_dep) / std_dep))
            msgsDF_anx = msgsDF_anx.withColumn(field_freq_field, ((F.col(field_freq_field) - mean_anx) / std_anx))
            msgsDF_ang = msgsDF_ang.withColumn(field_freq_field, ((F.col(field_freq_field) - mean_ang) / std_ang))

        # score = [(score - mean) / std] + mean
        if standardize and mean_center:
            print("Mean centering all scores")
            msgsDF_dep = msgsDF_dep.withColumn(field_freq_field, F.col(field_freq_field) + mean_dep)
            msgsDF_anx = msgsDF_anx.withColumn(field_freq_field, F.col(field_freq_field) + mean_anx)
            msgsDF_ang = msgsDF_ang.withColumn(field_freq_field, F.col(field_freq_field) + mean_ang)

        # Re-merge all feats
        msgsDF = msgsDF_dep.union(msgsDF_anx).union(msgsDF_ang)

        # apply floor and ceiling 1 additional time
        if post_standardize_fc:
            print("Applying floor of {} and ceiling of {} again".format(floor,ceiling))
            thresh_udf = udf(thresh_score, FloatType())
            msgsDF = msgsDF.withColumn(field_freq_field, thresh_udf(field_freq_field, feat_field))
       
    # Organize final dataframe
    msgsDF = msgsDF.select(col(yearweek_userid_field),col(feat_field),col(field_count_field),col(field_freq_field),
                           col(county_field),col(weight_field),col(yw_field))

    print("Processed data")
    msgsDF.show(20,False)

    print("Length of Processed Data:", msgsDF.count())

    # print("STOPPING EARLY")
    # sc.stop()
    # sys.exit(0)

    if rescaling and not standardize:
        #output_file = output_file.replace("weighted","{}{}scale".format(floor,ceiling))
        output_file = "{}.{}{}fc".format(output_file,floor,ceiling)
    if rescaling and standardize:
        #output_file = output_file.replace("weighted","{}{}scale".format(floor,ceiling))
        output_file = "{}.{}{}sc".format(output_file,floor,ceiling)
    if sigma_removal:
        output_file = output_file.replace("upts","upts{}sig".format(sigma))
    
    # Write to file
    print("Writing data to output file:", output_file)
    msgsDF.write.csv(output_file, quoteAll=False)

    # Close spark context
    sc.stop()
