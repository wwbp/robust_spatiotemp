# ~/spark/bin/spark-submit --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/filter_to_lex.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id --lex_file /home/smangalik/hadoop-tools/permaLexicon/dd_depAnxAng.csv

#!/usr/bin/env python
import numpy as np
import pandas as pd
import io, csv, re
import sys, os, argparse

from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F   

from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import udf, col, countDistinct

# Lexicon file to filter 1grams to, this is the largest of our dep/anx lexicons
DEF_LEX_FILE = "/home/smangalik/hadoop-tools/permaLexicon/dd_depAnxAng.csv"

DEF_INPUTFILE = None

DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__)) # '/hadoop-tools/sparkScripts'


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Collect stats about data")
    parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
                help='')
    parser.add_argument('--lex_file', dest='lex_file', default=DEF_LEX_FILE,
                help='Weighted lexicon csv file in hadoopPERMA/permaLexicon/ Default: %s' % (DEF_LEX_FILE))
    args = parser.parse_args()

    if not args.input_file:
        print("You must specify --input_file")
        sys.exit()

    input_file, header = args.input_file, False

    ## Spark Portion:
    session = SparkSession.builder\
            .config("spark.driver.memory","7g")\
            .appName("getStats")\
            .getOrCreate()
    sc = session.sparkContext

    # Read in terms
    lexicon = pd.read_csv(args.lex_file)
    lex_terms = list(set(lexicon['term']))
    print("Example lex terms",lex_terms[:50])
    print("Number of lex terms",len(lex_terms))

    # Read in tweets and display a sample
    ngramDF = session.read.csv(input_file, header=header)
    print("Original Data")
    ngramDF.show(n=20,truncate=False)
    print("Original Data Length:",ngramDF.count())

    # named fields
    groupid_field = "_c0"
    feat_field = "_c1"    
    count_field = "_c2"
    groupnorm_field = "_c3"

    # filter to only terms in lex_terms
    lexDF = ngramDF.where(col(feat_field).isin(lex_terms))
    # filter to only valid yw_user ids
    lexDF = lexDF.filter(~col(groupid_field).startswith(":"))
    lexDF = lexDF.filter(~col(groupid_field).endswith(":"))
    print("Filtered Data")
    lexDF.show(n=20,truncate=False)
    print("Filtered Data Length:",lexDF.count())

    # save out lex 1grams
    lex_name = args.lex_file.split("/")[-1].split(".")[0]
    output_file = input_file.replace("full",'lex'+lex_name[:3])
    lexDF.write.csv( output_file )
    print("Filtered data written to",output_file)



    # Close spark context
    sc.stop()