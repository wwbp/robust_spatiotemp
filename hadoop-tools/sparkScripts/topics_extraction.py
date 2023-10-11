'''
~/spark/bin/spark-submit --conf spark.executor.memory=7G --conf spark.executorEnv.PYTHONHASHSEED=323 --jars ~/hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file /home/smangalik/hadoop-tools/permaLexicon/dd_depAnxLex_ctlb2adapt_nostd.csv --word_table /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id
'''

#!/usr/bin/env python
import numpy as np
import io
import sys
import argparse
import csv
from pyspark import SparkContext
from pyspark.sql import SparkSession

DEF_LEX_FILE = "met_a30_2000_cp"
DEF_LEX_LOCATION = "/home/sgiorgi/hadoopPERMA/permaLexicon/"
DEF_WORD_TABLE = None
DEF_OUTPUTFILE = None
DEF_HEADER = True

def load_lex_csv(lex_file:str, header:bool):
    lex_dict = dict()
    _intercepts = dict()
    if ".csv" in lex_file:
        lfile = lex_file
    else:
        lfile = DEF_LEX_LOCATION + lex_file + ".csv"
    with open(lfile, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line in reader:
            if header:
                header = False
                continue
            term, cat, weight = line
            weight = float(weight)
            if term == '_intercept':
                print("Intercept detected %f [category: %s]" % (weight,cat))
                _intercepts[cat] = weight
            if term not in lex_dict:
                lex_dict[term] = {cat: weight}
            else:
                lex_dict[term][cat] = weight
    return lex_dict, _intercepts

def runLex(feats:dict, lex:dict, intercepts:dict):
    termCountLex = {} # count of any term appearing
    probLex = {} # prob of lex given user
    for cat in intercepts.keys(): # initialize values so the intercepts become default values
        termCountLex[cat] = 0.0
        probLex[cat] = 0.0
    #wtermCountLex = {} # weighted count of term appearing
    for term, freqs in feats.items(): # "happy" : (1.0, 0.002345)
        (count, group_norm) = freqs
        try:
            for cat, weight in lex[term].items(): # "ANX_SCORE" : 2.4582
                try:
                    termCountLex[cat] += float(count)
                    probLex[cat] += float(group_norm)*weight
                    # wtermCountLex[cat] += float(count)*weight
                except KeyError:
                    termCountLex[cat] = float(count)
                    probLex[cat] = float(group_norm)*weight
                    # wtermCountLex[cat] = float(count)*weight
        except KeyError:
            pass # term has no weight in the lex
    return {'term_counts':termCountLex, 'prob': probLex} #, 'weighted_term_counts': wtermCountLex}

def read_line_csv(line):
    try:
        #return csv.reader([l.replace('\0','').strip("(").strip(")").replace("'","").replace('"','') for l in line],delimiter=',')#, quotechar='"')
        return csv.reader([l.replace('\0','') for l in line],delimiter=',', quotechar='"')
    except:
        pass

def _list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL).writerow(x)
    return output.getvalue().strip() # remove extra newline

def write_csv(rdd, fileName):
    """
    Takes a dataframe, renames columns to standard feature tables names
    and writes to CSV
    """

    rdd.map(_list_to_csv_str).saveAsTextFile(fileName)
    print("Writing to: %s" % (fileName))

###############################
## Spark Portion:

def extract_topics(lex_file:str, word_table:str, output_file:str, lex_csv_header:bool):
    session = SparkSession\
            .builder\
            .appName("topicExtraction")\
            .getOrCreate()
    sc = session.sparkContext

    lex_dict, intercepts = load_lex_csv(lex_file, lex_csv_header)
    print("Reading: %s" % (lex_file))
    lex_dict_bc = sc.broadcast(lex_dict)
    intercepts_bc = sc.broadcast(intercepts)

    #load 1grams:
    print("Reading hdfs:%s" % (word_table))
    ngramsRdd = sc.textFile('hdfs:' + word_table).mapPartitions(lambda line: read_line_csv(line)).filter(lambda line: len(line) >= 4 and not line[1].startswith("@"))

    print("Original Data") 
    #for row in ngramsRdd.takeSample(False, 25): print(row[0],'\t',row[1],'\t',row[2],'\t',row[3])

    #run lexicon: ('2019_12356',{"sad":(2,0.000234)}) -> 
    print("Feat Data in Map format") 
    topicsRdd = ngramsRdd.map(lambda x: (x[0], {x[1]:(x[2], x[3])}))\
        .reduceByKey(lambda a, b: dict(a, **b), numPartitions=80)\
        .map(lambda tup: (tup[0], runLex(tup[1], lex_dict_bc.value, intercepts_bc.value)))
    #df = topicsRdd.toDF().show(n=20,truncate=False)

    # Apply intercepts
    print("Feat Data in Flattened CSV format") 
    topicsRddCSVStyle = topicsRdd\
        .flatMap(lambda tup: [(tup[0], cat, tup[1]['term_counts'][cat], gn + intercepts_bc.value.get(cat,0.0)) for cat, gn in tup[1]['prob'].items()])
    #df2 = topicsRddCSVStyle.toDF().show(n=20,truncate=False)

    print("Writing CSV Data to",output_file) 
    write_csv(topicsRddCSVStyle, output_file)

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Extract a weighted lexicon over a 1gram table")
    parser.add_argument('--lex_file', dest='lex_file', default=DEF_LEX_FILE,
                help='Weighted lexicon csv file in hadoopPERMA/permaLexicon. Default: %s' % (DEF_LEX_FILE))
    parser.add_argument('--word_table', dest='word_table', default=DEF_WORD_TABLE,
                help='Path to HDFS location of one gram table to run over. Default: %s' % (DEF_WORD_TABLE))
    parser.add_argument('--output_file', dest='output_file', default=None,
                help='HDFS location to store results Default: %s' % (None))
    parser.add_argument('--no_header', action='store_false', dest='lex_csv_header', default=DEF_HEADER,
                help='If lex CSV does not contain a header')
    args = parser.parse_args()

    output_file = args.output_file
    if output_file is None:
        lex_name = args.lex_file.split('/')[-1].replace('.csv','')
        output_file = args.word_table.replace('1gram',lex_name)
        print("No output file specified, using default: %s" % (output_file))

    if not (args.word_table):
        print("You must specify --word_table")
        sys.exit()


    extract_topics(args.lex_file, args.word_table, output_file, args.lex_csv_header)

# change to this at some point
# rdd_1gram.join(rdd_topic, by=('term','feat')).aggregate(key='category', sum(1gram_group_norm * topic_weight))