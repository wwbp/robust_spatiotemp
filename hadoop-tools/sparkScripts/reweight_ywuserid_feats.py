'''
run as
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/reweight_ywuserid_feats.py
'''


import csv, sys, argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Parameters - EDIT ME

# 2019
# DEF_INPUTFILE    = "/hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts.yw_user_id"
# DEF_OUTPUTFILE   = "/hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2019_full_3upts.yw_user_id"
# DEF_MAPPING_FILE = "/home/smangalik/post_strat_weights/users_2019/yw_user_2019_weights_income_k10_mbn50.csv" 

# 2020
DEF_INPUTFILE    = "/hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2020_full_3upts.yw_user_id"
DEF_OUTPUTFILE   = "/hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2020_full_3upts.yw_user_id"
DEF_MAPPING_FILE = "/home/smangalik/post_strat_weights/users_2020/yw_user_2020_weights_income_k10_mbn50.csv" 

# parse arguments
parser = argparse.ArgumentParser(description="Reweight yearweek_userids")
parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
            help='')
parser.add_argument('--output','--output_file', dest='output_file', default=DEF_OUTPUTFILE,
            help='')
parser.add_argument('--mapping_file', dest='mapping_file', default=DEF_MAPPING_FILE,
                help='Yearweek_Userid to Weight csv file. Default: %s' % (DEF_MAPPING_FILE))
args = parser.parse_args()

# Load args
feats               = args.input_file
output_file         = args.output_file
user_weight_mapping = args.mapping_file

# Create SparkContext
session = SparkSession\
            .builder\
            .appName("reweightFeats")\
            .getOrCreate()
sc = session.sparkContext

# Features
print("Loading Feats")
rdd = sc.textFile("hdfs:" + feats)\
    .map(lambda line: [str(x).strip('"') for x in line.split(",")])\
    .filter(lambda x: len(x) == 4)\
    .map( lambda x: ( x[0], x[1], float(x[2]), float(x[3]) ) )
# Show feature table
for e in rdd.take(5): print(e)

# Load user-county map
print("Loading yearweek_userid weight map")
ywuser_weights = {}
with open(user_weight_mapping) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        yw_userid, _, _, weight = str(row['user_id']),str(row['year_week']),str(row['cnty']),float(row['weight'])
        ywuser_weights[yw_userid] = weight
rand_key = list(ywuser_weights.keys())[0] # "2019_49:69683787"
print("Ex.",rand_key,"->",ywuser_weights[rand_key])

# Yields chunks of size n from a list
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield set(lst[i:i + n])

# Create yearweek_userid Sets
all_yw_users = list(ywuser_weights.keys())
n_split = 10
ywuser_sets = list(chunks(all_yw_users, len(all_yw_users) // n_split))
print("Split Lengths:",[len(x) for x in ywuser_sets])

# print("STOPPING EARLY")
# sc.stop()
# sys.exit()

# Filter function -- needs to catch bad tuples with non-integer user_id -- prints what's excluded just to make sure we're not throwing out things we should keep
def filter_func(x, vals):
    try:
        return x in vals # "2020_9:999999926" in mapping?
    except ValueError:
        print('ValueError:', str(x))
        return False

for i, ywuser_set in enumerate(ywuser_sets):
    print('Iter {i}:'.format(i=i), list(ywuser_set)[:5])

    # Broadcast subset of mapping
    ywuser_weights_dict = {ywuser_k:weight_v for ywuser_k,weight_v in ywuser_weights.items() if ywuser_k in ywuser_set}
    print("-> Reweighting over",len(ywuser_weights_dict),"ywuser-weight pairs")
    ywuser_weights_dict_bc = sc.broadcast(ywuser_weights_dict)

    # Filter to only valid values using filter_func() then reweight value and group_norm using map()
    iter_result = rdd.filter(lambda x: filter_func(x[0], ywuser_weights_dict_bc.value.keys()))\
        .map( lambda x: ( x[0], x[1], ywuser_weights_dict_bc.value[x[0]] * x[2], ywuser_weights_dict_bc.value[x[0]] * x[3] ) )\
        .persist()

    # As dataframe -- allows .show()
    #result_df = iter_result.toDF()
    #result_df.sample(False, 0.01, seed=0).limit(10).show()
    iter_result.toDF().write.csv("hdfs:" + output_file + "_" + str(i)) # From DataFrame

    # Write out the result
    #iter_result.saveAsTextFile("hdfs:" + output_file + "_" + str(i)) # From RDD
    iter_result.unpersist()

sc.stop()
