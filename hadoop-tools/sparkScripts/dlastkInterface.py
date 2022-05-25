#!/usr/bin/env python3

"""
Interface module for DLASTK (Differential Language Analysis Spark ToolKit)
"""

import sys, os
import io
import argparse
import csv
import time
import re
import getpass

from operator import add
from collections import Counter
from html.parser import HTMLParser

import numpy as np

from langid.langid import LanguageIdentifier, model

# dlatk imports
from dlatk.lib.happierfuntokenizing import Tokenizer

# Spark Imports
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

##### Defaults
# Corpus defaults
DEF_INPUT_PATH = '/user/' + getpass.getuser()
DEF_MSG_TABLE = 'msgs'
DEF_MSG_IDX = 1
DEF_CORR_IDX = 0
DEF_CORR_NAME = 'user_id'

# Feature defaults
DEF_N = 1
DEF_LEX_FILE = "met_a30_2000_cp"
DEF_LEX_LOCATION = "/home/sgiorgi/hadoopPERMA/permaLexicon/"
DEF_WORD_TABLE = None

# Outcome defaults
DEF_OUTPUTFILE = None

DEFAULT_PATH = '/home/sgiorgi/hadoopPERMA/sparkScripts/lib/'
TOKENIZER_PATH = DEFAULT_PATH + 'happierfuntokenizing.py'

LOWERCASE_ONLY = True

### Methods

# csv methods
def read_line_csv(line):
    try:
        return csv.reader(line)
    except:
        pass

def _list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline

def write_csv(rdd, tableName):
    """Takes a dataframe, renames columns to standard feature tables names
    and writes to CSV"""

    print("SPARK QUERY: Writing to: %s" % (tableName))
    rdd.map(_list_to_csv_str).saveAsTextFile(tableName)

# lib files
def _addTokenizerFile(sc):
    sc.addPyFile(TOKENIZER_PATH) 

# text cleaning methods
newlines = re.compile(r'\s*\n\s*')
multSpace = re.compile(r'\s\s+')
startSpace = re.compile(r'^\s+')
endSpace = re.compile(r'\s+$')
multDots = re.compile(r'\.\.\.\.\.+') #more than four periods
newlines = re.compile(r'\s*\n\s*')

def removeNonUTF8(s): 
    """remove non-utf8 values from string s and replace with <NON-UTF8>"""
    if s:
        new_words = []
        for w in s.split():
            if len(w.encode("utf-8",'ignore').decode('utf-8','ignore')) < len(w):
                new_words.append("<NON-UTF8>")
            else:
                new_words.append(w)
        return " ".join(new_words)
    return ''

def shrinkSpace(s):
    """turns multipel spaces into 1"""
    s = multSpace.sub(' ',s)
    s = multDots.sub('....',s)
    s = endSpace.sub('',s)
    s = startSpace.sub('',s)
    s = newlines.sub(' <NEWLINE> ',s)
    return s

def treatNewlines(s):
    s = newlines.sub(' <NEWLINE> ',s)
    return s

def regex_or(*items):
    r = '|'.join(items)
    r = '(' + r + ')'
    return r

def pos_lookahead(r):
    return '(?=' + r + ')'

def optional(r):
    return '(%s)?' % r

mycompile = lambda pat:  re.compile(pat,  re.UNICODE)
PunctChars = r'''['“".?!,:;]'''
Entity = '&(amp|lt|gt|quot);'
EmoticonsDN= '(:\)|:\(|:-\)|>:]|:o\)|:3|:c\)|:>|=]|8\)|=\)|:}|:^\)|>:D\)|:-D|:D|8-D|8D|x-D|xD|X-D|XD|=-D|=D|=-3|=3\)|8-\)|:-\)\)|:\)\)|>-\[|:-\(|:\(|:-c|:c|:-<|:<|:-\[|:\[|:{|>.>|<.<|>.<|:-\|\||D:<|D:|D8|D;|D=|DX|v.v|D-\':|>;\]|;-\)|;\)|\*-\)|\*\)|;-\]|;\]|;D|;^\)|>:P|:-P|:P|X-P|x-p|xp|XP|:-p|:p|=p|:-b|:b|>:o|>:O|:-O|:O|:0|o_O|o_0|o.O|8-0|>:\\|>:/|:-/|:-.|:/|:\\|=/|=\\|:S|:\||:-\||>:X|:-X|:X|:-#|:#|:$|O:-\)|0:-3|0:3|O:-\)|O:\)|0;^\)|>:\)|>;\)|>:-\)|:\'-\(|:\'\(|:\'-\)|:\'\)|;\)\)|;;\)|<3|8-}|>:D<|=\)\)|=\(\(|x\(|X\(|:-\*|:\*|:\">|~X\(|:-?)'
UrlStart1 = regex_or('https?://', r'www\.')
CommonTLDs = regex_or('com','co\\.uk','org','net','info','ca', 'co')
UrlStart2 = r'[a-z0-9\.-]+?' + r'\.' + CommonTLDs + pos_lookahead(r'[/ \W\b]')
UrlBody = r'[^ \t\r\n<>]*?'  # * not + for case of:  "go to bla.com." -- don't want period
UrlExtraCrapBeforeEnd = '%s+?' % regex_or(PunctChars, Entity)
UrlEnd = regex_or( r'\.\.+', r'[<>]', r'\s', '$')
Url = regex_or((r'[a-z0-9\.-]+?' + r'\.' + CommonTLDs + UrlEnd),(r'\b' +
    regex_or(UrlStart1, UrlStart2) +
    UrlBody +
    pos_lookahead(optional(UrlExtraCrapBeforeEnd) + UrlEnd)))

NumNum = r'\d+\.\d+'
NumberWithCommas = r'(\d+,)+?\d{3}' + pos_lookahead(regex_or('[^,]','$'))
Punct = '%s+' % PunctChars
Separators = regex_or('--+', '―')
Timelike = r'\d+:\d+h{0,1}' # removes the h trailing the hour like in 18:00h
Number = r'^\d+'
OneCharTokens = r'^.{1}$' # remove the one character tokens (maybe too agressive)
ParNumber = r'[()][+-]*\d+[()]*' # remove stuff like (+1 (-2 that appear as tokens

ExcludeThese = [
    EmoticonsDN,
    Url,
    NumNum,
    NumberWithCommas,
    Punct,
    Separators,
    Timelike,
    Number,
    OneCharTokens,
    ParNumber
]
Exclude_RE = mycompile(regex_or(*ExcludeThese))

def rttext(message):
    """
    """
    regnrt = re.compile(r"\(*RT[\s!.-:]*@\w+([\)\s:]|$)")
    regrt = re.compile(r"^RT[\s!.-:]+")
    reguser = re.compile(r"@\w+")
    regbr = re.compile(r"\[.*\]")
    regv1 = re.compile(r"\(via @\w+\)")
    regv2 = re.compile(r" via @\w+")
    rt = ''
    com = ''
    c = regnrt.search(message)
    if c:
        rt = message[c.span()[1]:].strip().strip(':').strip()
        com = message[:c.span()[0]].strip().strip(':').strip()
        if c.span()[1] == len(message):
            aux = com
            com = rt
            rt = aux
    else:
        d = regrt.search(message)
        e = reguser.search(message)
        if d and e:
            com = message[d.span()[1]:e.span()[0]]
            rt = message[e.span()[1]:]
    a = regv1.search(message)
    if not a:
        a = regv2.search(message)
    if a:
        if a.span()[0] == 0:
            b = regbr.search(message)
            rt = re.sub('^:','',message[a.span()[1]:b.span()[0]].strip()).strip()
            com = b.group()[1:len(b.group())-1]
        else:
            rt = re.sub('[|,.//]$','',message[:a.span()[0]].strip()).strip()
            com = re.sub('^:','',message[a.span()[1]:].strip()).strip()
    return rt, com

def replaceURL(message):
    message = re.sub(Url, "<URL>", message)
    return message

def replaceUser(message):
    message = re.sub(r"@\w+", "<USER>", message)
    return message


# extraction methods
def getMessagesForCorrelField(sc, input_file, corr_idx, msg_idx, header=False):
    """Reads a csv and returns a dataframe with message and correl field columns"""
    print("SPARK QUERY: Reading from: %s" % (input_file))
    rdd = sc.textFile(input_file)
    rdd = rdd.mapPartitions(lambda x: csv.reader(x))

    return (rdd.map(lambda line: [line[corr_idx],line[msg_idx]] if (line[corr_idx] and line[msg_idx]) else ["null", "null"])
        )

def getNgramPerRow(row, n):
    try:
        grp_id = row[0]
        if grp_id:
            return (row[0], Counter([" ".join(x) for x in zip(*[row[1][i:] for i in range(n)])]))
        else:
            pass
    except:
        pass
    

def extract_ngrams(sc, input_file, featTableName, n, corr_idx, msg_idx, lowercase_only=True, min_freq=1, tableName = None, valueFunc = lambda d: d, metaFeatures = True):
    """"""
    tokenizer = Tokenizer(use_unicode=True)

    # read csv, grabbing only message and correl field
    rdd = getMessagesForCorrelField(sc=sc, input_file=input_file, corr_idx=corr_idx, msg_idx=msg_idx)

    # apply filters and tokenize messages
    rdd = (rdd.map(lambda row: [row[0],
        tokenizer.tokenize(removeNonUTF8(shrinkSpace(treatNewlines(row[1]))))])
        )

    # extract ngrams from tokenized messages, apply Counter and add Counters
    grouped = rdd.map(lambda row: getNgramPerRow(row,n)).reduceByKey(add)

    # flatten RDD (convert to DF) so each row is (group_id, feat, value, group_norm)
    flat = grouped.flatMap(lambda row: [[row[0], feat, value, valueFunc(value/sum(row[1].values()))] for feat, value in row[1].items()  if value >= min_freq])

    # write to csv
    write_csv(flat, featTableName)

def load_lex_csv(lex_file, header=True):
    lex_dict = dict()
    _intercepts = dict()
    if ".csv" in lex_file:
        lfile = DEF_LEX_LOCATION + lex_file
    else:
        lfile = DEF_LEX_LOCATION + lex_file + ".csv"
    with open(lfile, "r") as f:
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

def runLex(feats, lex):
    pLex = {}#prob of lex given user
    countLex = {}#count of any term appearing
    wCountLex = {}#weighted count of term appearing
    for term, freqs in feats.items():
        (c, gn) = freqs
        try:
            for cat, weight in lex[term].items():
                try:
                    pLex[cat] += float(gn)*weight
                    countLex[cat] += int(c)
                    wCountLex[cat] += float(c)*weight
                except KeyError:
                    pLex[cat] = float(gn)*weight
                    countLex[cat] = int(c)
                    wCountLex[cat] = float(c)*weight
        except KeyError:
            pass #not in lex
    return {'prob': pLex, 'term_counts':countLex, 'weighted_term_counts': wCountLex}

def addLexiconFeat(sc, lex_file, word_table, output_file, lex_csv_header=True):
    """"""
    lex_dict, intercepts = load_lex_csv(lex_file, lex_csv_header)
    print("Done Reading: %s" % (lex_file))
    lex_dict_bc = sc.broadcast(lex_dict)
    intercepts_bc = sc.broadcast(intercepts)

    #load 1grams:
    ngramsRdd = sc.textFile('hdfs:' + word_table).mapPartitions(lambda line: read_line_csv(line))

    #run lexicon:
    topicsRdd = ngramsRdd.map(lambda x: (x[0], {x[1]:(x[2], x[3])}))\
        .reduceByKey(lambda a, b: dict(a, **b), numPartitions=80)\
        .map(lambda tup: (tup[0], runLex(tup[1], lex_dict_bc.value)))

    topicsRddCSVStyle = topicsRdd\
        .flatMap(lambda tup: [(tup[0], cat, tup[1]['term_counts'][cat], gn + intercepts_bc.value.get(cat,0)) for cat, gn in tup[1]['prob'].items()])

    write_csv(topicsRddCSVStyle, output_file)

# message cleaning methods
def parsed_message(s):
    ""
    s = replaceUser(replaceURL(s))
    words = [word for word in tokenizer.tokenize(s) if ((word[0]!='#') and (word[0]!='@') and not Exclude_RE.search(word))]
    s = ' '.join(words).lower()
    if len(words)>=6:
        if 'YouTube' in words:
            s = ' '.join(words[0:5])
        else:
            s = ' '.join(words[0:6])
    return s

def addDedupFilterTable(sc, session, input_file, output_file, msg_idx, corr_idx, clean_messages=False, header=False):
    """"""
    #_addTokenizerFile(sc)
    #from happierfuntokenizing import Tokenizer
    
    print("SPARK QUERY: Reading messages from %s" % (input_file))
    msgsDF = session.read.csv(input_file, header=header)

    dup_field = 'temp_column'
    message_field = "_c" + str(msg_idx)
    group_field = "_c" + str(corr_idx)

    # does the deduplication
    dedupDF = msgsDF.withColumn(dup_field, udf(parsed_message, StringType())(message_field)).\
              dropDuplicates([group_field, dup_field]).drop(dup_field)
    
    # replaces URLs and USERs with constant text
    if clean_messages:
        dedupDF = dedupDF.withColumn(message_field, udf(lambda s: replaceUser(replaceURL(s)), StringType())(message_field))
    dedupDF.write.csv(output_file, quoteAll=True)

    # dedupRDD = dedupDF.rdd

    # write_csv(dedupRDD, output_file)

def language_filter_message(message):
    ""
    identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    html = HTMLParser()

    message = html.unescape(message)
    message = re.sub(r"(?:\#|\@|https?\://)\S+", "", message)

    message = message.lower()

    lang, conf = identifier.classify(message)

    return conf

def addLanguageFilterTable(sc, session, input_file, output_file, msg_idx, corr_idx, langs, clean_messages=False, lowercase=True, header=False):
    """Filters all messages in corptable for a given language. Keeps messages if
    confidence is greater than 80%. Uses the langid library.

    Parameters
    ----------
    langs : list
        list of languages to filter for
    cleanMessages : boolean
        remove URLs, hashtags and @ mentions from messages before running langid
    lowercase : boolean
        convert message to all lowercase before running langid
    """
    
    print("SPARK QUERY: Reading messages from %s" % (input_file))
    msgsDF = session.read.csv(input_file, header=header)

    temp_field = 'temp_column'
    message_field = "_c" + str(msg_idx)
    group_field = "_c" + str(corr_idx)

    # does the deduplication
    langDF = msgsDF.withColumn(temp_field, udf(language_filter_message, StringType())(message_field)).filter("%s > .80" % temp_field)
    
    # replaces URLs and USERs with constant text
    if clean_messages:
        langDF = langDF.withColumn(message_field, udf(lambda s: replaceUser(replaceURL(s)), StringType())(message_field))
    
    #langRDD = langDF.rdd

    #write_csv(langRDD, output_file)
    print("SPARK QUERY: Writing to %s" % (output_file))
    langDF.write.csv(output_file, quoteAll=True)
    #langDF.write.options("quoteAll", "true").format("csv")

#################################################################
### Main / Command-Line Processing:
##
#
if __name__ == '__main__':

    start_time = time.time()

    # Argument Parser
    parser = argparse.ArgumentParser(description="Extract and Manage Language Feature Data.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    group = parser.add_argument_group('Corpus Variables', 'Defining the data from which features are extracted.')
    group.add_argument('-d', '--input_path', '--input', '-i', dest='input_path', default=DEF_INPUT_PATH,
                help='Path to HDFS directory where your message table lives.')
    group.add_argument('-t', '--corptable', metavar='TABLE', dest='corptable', default=DEF_MSG_TABLE,
                        help='Name of message table located in the -d directory.')
    group.add_argument('-c_name', '--correl_name', '--group_name', dest='corr_name', type=str, default=DEF_CORR_NAME,
                help='Correlation Field name (AKA Group Field): The field which features are aggregated over.')
    group.add_argument('-c_index', '-c_idx', '--correl_idx', '--group_idx', dest='corr_idx', type=int, default=DEF_CORR_IDX,
                help='Correlation Field index: column index in csv')
    group.add_argument('-m', '--message_field', '--msg_idx', dest='msg_idx', type=int, default=DEF_MSG_IDX,
                help='The field index where the text to be analyzed is located.')


    group = parser.add_argument_group('Feature Variables', 'Use of these is dependent on the action.')
    group.add_argument('-n', '--set_n', metavar='N', dest='n', type=int, nargs='+', default=[DEF_N],
                help='The n value used for n-grams')
    group.add_argument('-l', '--lex_file', dest='lex_file', default=DEF_LEX_FILE,
                help='Weighted lexicon csv file in hadoopPERMA/permaLexicon.')
    group.add_argument('--word_table', dest='word_table', default=DEF_WORD_TABLE,
                help='Path to HDFS location of one gram table to run over.')
    
    group = parser.add_argument_group('Outcome Variables', '')
    group.add_argument('-o', '--output_file', '--output', dest='output_file', default=DEF_OUTPUTFILE,
                help='HDFS location to store results. This will override any default output paths.')

    group = parser.add_argument_group('Standard Extraction Actions', '')
    group.add_argument('--add_ngrams', action='store_true', dest='addngrams',
                       help='add an n-gram feature table. (uses: n, can flag: sqrt), gzip_csv'
                       'can be used with or without --use_collocs')
    group.add_argument('--add_lex_table', action='store_true', dest='addlextable',
                       help='add a lexicon-based feature table. (uses: l, weighted_lexicon, can flag: anscombe).')
    group.add_argument('--no_lower', action='store_false', dest='lowercaseonly', default=LOWERCASE_ONLY,
                       help='')

    group = parser.add_argument_group('Message Cleaning Actions', '')
    group.add_argument('--language_filter', '--lang_filter',  type=str, metavar='FIELD(S)', dest='langfilter', nargs='+', default=[],
                       help='Filter message table for list of languages.')
    group.add_argument('--deduplicate', action='store_true', dest='deduplicate',
                       help='Removes duplicate messages within correl_field grouping, writes to new table corptable_dedup Not to be run at the message level.')
    group.add_argument('--clean_messages', dest='cleanmessages', action = 'store_true', help="Remove URLs, hashtags and @ mentions from messages during deduplication")

  
    args = parser.parse_args()

    sc = SparkContext()
    session = SparkSession\
            .builder\
            .appName("dlastk")\
            .getOrCreate()

    if args.input_path.endswith("/"):
        args.input_path = args.input_path[:-1]

    ### basic feature extraction
    # extract ngrams
    if args.addngrams:
        for n in args.n:
            input_file = args.input_path + "/" + args.corptable
            if not args.output_file: output_file = args.input_path + "/" + ".".join(["feat", str(n) + "gram", args.corptable, args.corr_name])
            print("Extracting %sgrams to %s" % (str(n), output_file))
            extract_ngrams(sc, input_file, output_file, n, args.corr_idx, args.msg_idx)

    # extract lexica
    if args.addlextable:
        if not (args.word_table): 
            args.word_table = args.input_path + "/" + ".".join(["feat", "1gram", args.corptable, args.corr_name])
        if not args.output_file: output_file = args.input_path + "/" + ".".join(["feat", args.lex_file, args.corptable, args.corr_name])
        addLexiconFeat(sc, args.lex_file, args.word_table, output_file)

    # message cleaning
    if args.langfilter:
        input_file = args.input_path + "/" + args.corptable
        if not args.output_file: output_file = args.input_path + "/" + args.corptable + "_" + str(args.langfilter[0])
        addLanguageFilterTable(sc=sc, session=session, input_file=input_file, output_file=output_file, msg_idx=args.msg_idx, corr_idx=args.corr_idx, langs=args.langfilter, clean_messages=args.cleanmessages, lowercase=args.lowercaseonly)

    if args.deduplicate:
        input_file = args.input_path + "/" + args.corptable
        if not args.output_file: output_file = args.input_path + "/" + args.corptable + "_dedup"
        tokenizer = Tokenizer(use_unicode=True)
        addDedupFilterTable(sc=sc, session=session, input_file=input_file, output_file=output_file, msg_idx=args.msg_idx, corr_idx=args.corr_idx, clean_messages=args.cleanmessages)


    print("--\nInterface Runtime: %.2f seconds"% float(time.time() - start_time))
    print("DLASTK exits with success! A good day indeed.")