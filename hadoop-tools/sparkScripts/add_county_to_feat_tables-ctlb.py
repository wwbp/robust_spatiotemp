# ~/spark/bin/spark-submit --conf spark.executor.memory=7G --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/add_county_to_feat_tables-ctlb.py --word_table /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_scaled.timelines2019_full_3upts.yw_user_id

import csv, sys, argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Lexicon Experiments
DEF_WORD_TABLE = "/hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_1upts.yw_user_id"
DEF_MAPPING_FILE = "/home/smangalik/user_county_mapping.csv"

parser = argparse.ArgumentParser(description="Add the county column to CTLB2")
parser.add_argument('--mapping_file', dest='mapping_file', default=DEF_MAPPING_FILE,
                help='Yearweek_Userid to County mapping csv file. Default: %s' % (DEF_MAPPING_FILE))
parser.add_argument('--word_table', dest='word_table', default=DEF_WORD_TABLE,
            help='Path to HDFS location of one gram table to run over. Default: %s' % (DEF_WORD_TABLE))
args = parser.parse_args()

# Load args
feats          = args.word_table
output_file    = args.word_table.replace("*","") + ".cnty"
county_mapping = args.mapping_file

# Create SparkContext
session = SparkSession\
            .builder\
            .appName("addCountyToFeats")\
            .getOrCreate()
sc = session.sparkContext

# Features
rdd = sc.textFile("hdfs:" + feats).map(lambda line: line.split(",")).filter(lambda x: len(x)>=4)
rdd.toDF().show(n=10,truncate=False)

print("Loaded feature table",feats)

# Load user-county map
mapping = {}
with open(county_mapping) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        user_id, cnty = int(row['user_id']),int(row['cnty'])
        mapping[user_id] = cnty

# Yields chunks of size n from a list
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield set(lst[i:i + n])

# Create yearweek_userid Sets
n_split = 20
all_counties = list(set(mapping.values()))
cnty_sets = list(chunks(all_counties, len(all_counties) // n_split))
print("Loaded county map with",len(mapping),"entries")
print("Partition Lengths:",[len(x) for x in cnty_sets])

# "2020_9:999999926" -> 999999926
def yw_userid_to_userid(x):
    try:
        userid = int(x.split(":")[-1].replace('"',''))
        return userid
    except ValueError:
        print('ValueError:', str(x))
        return False

# Filter function -- needs to catch bad tuples with non-integer user_id -- prints what's excluded just to make sure we're not throwing out things we should keep
def filter_func(x, vals):
    return yw_userid_to_userid(x) in vals # "2020_9:999999926" -> 999999926 -> True/False



# Use on Total to get county coverage
# print("All possible counties", len(set(mapping.values())))
# all_users = df.iloc[:, 0]
# df = rdd.map(lambda x: (x[0], x[1])).toDF()
# df.show()
# df.select('_2').distinct().toPandas()['_2'].to_csv('all_users_2019.csv',index=False)
# all_users = list(df.select('_2').distinct().toPandas()['_2'])
# df.unpersist(blocking=True)
# rdd.unpersist()
# print(all_users[:10])
# county_coverage = set()
# for user in all_users:
#     county_covered = mapping.get(int(user.replace('"','').replace("'","")),None)
#     if county_covered is not None: county_coverage.add(county_covered)
# print("Covered counties", len(county_coverage))

# print("STOPPING EARLY")
# sc.stop()
# sys.exit()

# Add county to observations and save result
for i, cnty_set in enumerate(cnty_sets):
    print('Iter {i}:'.format(i=i), list(cnty_set)[:10])

    #users_en_dict = mapping.loc[mapping.cnty.apply(lambda x: x in cnty_set)].filter(items=['user_id','cnty']).set_index('user_id').to_dict()['cnty']
    mapping_dict = {user_k:county_v for user_k,county_v in mapping.items() if county_v in cnty_set}
    print("-> Searching over",len(mapping_dict),"user-county pairs")
    mapping_dict_bc = sc.broadcast(mapping_dict)

    # Filter to only valid values using filter_func() then add the county column using map()
    
    # for feat table 
    #iter_result = rdd.filter(lambda x: filter_func(x[0], mapping_dict_bc.value.keys())).map(lambda x: (x[0], x[1], x[2], x[3], mapping_dict_bc.value[yw_userid_to_userid(x[0])])).persist()
    
    # for raw data
    #iter_result = rdd.filter(lambda x: filter_func(x[1], mapping_dict_bc.value.keys())).map(lambda x: (x[0], x[1], x[2], x[3], mapping_dict_bc.value[yw_userid_to_userid(x[1])])).persist()
    
    # for upt data
    iter_result = rdd.filter(lambda x: filter_func(x[0], mapping_dict_bc.value.keys())).map(lambda x: (x[0], x[1], x[2], x[3], x[4], mapping_dict_bc.value[yw_userid_to_userid(x[0])])).persist()
    
    #iter_result.toDF().show(n=20,truncate=False) # peek at the result

    # print("STOPPING EARLY")
    # sc.stop()
    # sys.exit()

    #print("Added counties to",iter_result.count(),"rows")

    #iter_result.saveAsTextFile(output_file + "_" +str(i)) # store in RDD format
    iter_output_file = output_file + "_" + str(i)
    print("Writing to",iter_output_file)
    iter_result.toDF().write.csv(iter_output_file, quoteAll=True)
    iter_result.unpersist()

    

sc.stop()