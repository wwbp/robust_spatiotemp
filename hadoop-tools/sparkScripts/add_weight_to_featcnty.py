'''
run as
~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/add_weight_to_featcnty.py --input_file /hadoop_data/ctlb/2019/feats/featANS.dd_daa_c2adpt_ans_nos.timelines2019_full_3upts.yw_user_id.cnty --mapping_file /home/smangalik/post_strat_weights/users_2019/yw_user_2019_weights_income_k10_mbn50.csv
'''
import csv, sys, argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from tqdm import tqdm

# Parameters - EDIT ME

# 2019
DEF_INPUTFILE    = "/hadoop_data/ctlb/2019/feats/featANS.dd_daa_c2adpt_ans_nos.timelines2019_full_3upts.yw_user_id.cnty"
DEF_MAPPING_FILE = "/home/smangalik/post_strat_weights/users_2019/yw_user_2019_weights_income_k10_mbn50.csv" 

# 2020
#DEF_INPUTFILE    = "/hadoop_data/ctlb/2020/feats/featANS.dd_daa_c2adpt_ans_nos.timelines2020_full_3upts.yw_user_id.cnty"
#DEF_MAPPING_FILE = "/home/smangalik/post_strat_weights/users_2020/yw_user_2020_weights_income_k10_mbn50.csv" 

# parse arguments
parser = argparse.ArgumentParser(description="Add weights based on yearweek_userids")
parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
            help='')
parser.add_argument('--mapping_file', dest='mapping_file', default=DEF_MAPPING_FILE,
                help='Yearweek_Userid to Weight csv file. Default: %s' % (DEF_MAPPING_FILE))
args = parser.parse_args()

# Load args
feats               = args.input_file
output_file         = feats + '.wt'
user_weight_mapping = args.mapping_file

# Create SparkContext
session = SparkSession\
            .builder\
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")\
            .config("spark.driver.memory", "50g")\
            .config("spark.executor.memory", "50g")\
            .appName("addWeightToFeats")\
            .getOrCreate()
sc = session.sparkContext

# Features
print("Loading Feats")
rdd = sc.textFile("hdfs:" + feats)\
    .map(lambda line: [str(x).strip('"') for x in line.split(",")])\
    .filter(lambda x: len(x) == 5 and not x[0].startswith(":"))\
    .map( lambda x: ( x[0], x[1], float(x[2]), float(x[3]), x[4] ) )
# Show feature table
for e in rdd.take(5): print(e)

# Load user-county map
print("Loading yearweek_userid weight map")
ywuser_weights = {}
with open(user_weight_mapping) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in tqdm(reader):
        yw_userid, _, _, weight = str(row['user_id']),str(row['year_week']),str(row['cnty']),float(row['weight'])
        if yw_userid.startswith(':'): 
            continue
        ywuser_weights[yw_userid] = weight
for rand_key in list(ywuser_weights.keys())[:5]: 
    print("Ex.",rand_key,"->",ywuser_weights[rand_key]) # "2019_49:69683787"
print(len(ywuser_weights), "ywuser-weight mappings")

# For all unique entries in rdd that are not in ywuser_weights, add them with weight 1.0
print("Searching for missing yearweek_userid weight map entries")
missing_ywuser_weights = {}
# def extract_first(iterator):
#     for item in iterator:
#         yield item[0]
# rdd_wts = sc.parallelize(list(ywuser_weights), numSlices=10000)
# unique_ywuserids = rdd.mapPartitions(extract_first).subtract(rdd_wts).distinct().collect()
unique_ywuserids = rdd.map(lambda x: x[0]).distinct().collect()
# rdd_wts.unpersist()
print("Processing",abs(len(unique_ywuserids) - len(ywuser_weights)),"missing entries")
for yw_userid in set(unique_ywuserids) - set(list(ywuser_weights)):
    if yw_userid.startswith(':'): 
        continue
    missing_ywuser_weights[yw_userid] = 1.0
print("Adding",len(missing_ywuser_weights),"missing entries")
print("Ex.",list(missing_ywuser_weights.keys())[:5],"->",list(missing_ywuser_weights.values())[:5])
ywuser_weights.update(missing_ywuser_weights)


# Yields chunks of size n from a list
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield set(lst[i:i + n])

# Create yearweek_userid Sets
all_yw_users = list(ywuser_weights.keys())
n_split = 20
ywuser_sets = list(chunks(all_yw_users, len(all_yw_users) // n_split))
print("Split Lengths:",[len(x) for x in ywuser_sets])

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


    # Filter to only valid values using filter_func() then add weight columns 
    iter_result = rdd.filter(lambda x: filter_func(x[0], ywuser_weights_dict_bc.value.keys()))\
        .map( lambda x: ( x[0], x[1], x[2], x[3], x[4], ywuser_weights_dict_bc.value[x[0]] ) )
    for e in iter_result.take(3): print(e)

    # As dataframe -- allows .show()
    #result_df = iter_result.toDF()
    #result_df.sample(False, 0.01, seed=0).limit(10).show()
    iter_result_file = "hdfs:" + output_file + "_" + str(i)
    iter_result.toDF().write.csv(iter_result_file,header=False) # From DataFrame
    print("Writing out to",iter_result_file)

    # Write out the result
    #iter_result.saveAsTextFile("hdfs:" + output_file + "_" + str(i)) # From RDD
    # iter_result.unpersist()
    #ywuser_weights_dict_bc.destroy()

# # Assign a weight of 1.0 to all remaining entries
# print("Adding weight 1.0 to all remaining entries")
# # add column of all 1.0s
# remaining_df = remaining_df.withColumn('weight', lit(1.0))
# for e in iter_result.take(3): print(e)
# iter_result_file = "hdfs:" + output_file + "_not_in_mapping"
# print("Writing out to",iter_result_file)
# iter_result.toDF().write.csv(iter_result_file,header=False) # From DataFrame

sc.stop()
