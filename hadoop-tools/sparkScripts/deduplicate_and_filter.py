#!/usr/bin/env python
import numpy as np
import io
import sys, os
import argparse
import csv
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, BooleanType
from pyspark.sql.functions import udf

from pyspark.sql import functions as F

DEF_INPUTFILE = None
DEF_OUTPUTFILE = None
DEF_MSG_FIELD = 1
DEF_GROUP_FIELD = 0
DEF_ANNON = False

DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__)) # '/hadoop-tools/sparkScripts'
TOKENIZER_PATH = DEFAULT_PATH + '/lib/happierfuntokenizing.py'

import re
import html.entities

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most imporatantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8>]                    # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpPxX/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpPxX/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8<]                    # eyes
      [<>]?
      |
      <[/\\]?3                         # heart(added: has)
      |
      \(?\(?\#?                   #left cheeck
      [>\-\^\*\+o\~]              #left eye
      [\_\.\|oO\,]                #nose
      [<\-\^\*\+o\~]              #right eye
      [\#\;]?\)?\)?               #right cheek
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # http:
    # Web Address:
    r"""(?:(?:http[s]?\:\/\/)?(?:[\w\_\-]+\.)+(?:com|net|gov|edu|info|org|ly|be|gl|co|gs|pr|me|cc|us|gd|nl|ws|am|im|fm|kr|to|jp|sg)(?:\/[\s\b$])?)"""
    ,
    r"""(?:http[s]?\:\/\/)"""   #need to capture it alone sometimes
    ,
    #command in parens:
    r"""(?:\[[\w_]+\])"""   #need to capture it alone sometimes
    ,
    # HTTP GET Info
    r"""(?:\/\w+\?(?:\;?\w+\=\w+)+)"""
    ,
    # HTML tags:
    r"""(?:<[^>]+\w=[^>]+>|<[^>]+\s\/>|<[^>\s]+>?|<?[^<\s]+>)"""
    #r"""(?:<[^>]+\w+[^>]+>|<[^>\s]+>?|<?[^<\s]+>)"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[\w][\w'\-_]+[\w])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

hex_re = re.compile(r'\\x[0-9a-z]{1,4}')

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False, use_unicode=True):
        self.preserve_case = preserve_case
        self.use_unicode = use_unicode

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        if self.use_unicode:
            try:
                s = str(s)
            except UnicodeDecodeError:
                s = str(s).encode('string_escape')
                s = str(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        s = self.__removeHex(s)
        # Tokenize:
        words = word_re.findall(s)
        #print words #debug
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = list(map((lambda x : x if emoticon_re.search(x) else x.lower()), words))
        
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print("Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/")
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, chr(entnum)) 
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = list(filter((lambda x : x != amp), ents))
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, chr(html.entities.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s

    def __removeHex(self, s):
        return hex_re.sub(' ', s)


###############################################################################

if __name__ == '__main__':
    #tok = Tokenizer(preserve_case=True)
    tok = Tokenizer(preserve_case=False)

    import sys

    samples = (
        "RT @ #happyfuncoding: this is a typical Twitter tweet :-)",
        "It's perhaps noteworthy that phone numbers like +1 (800) 123-4567, (800) 123-4567, and 123-4567 are treated as words despite their whitespace.",
        'Something </sarcasm> about <fails to break this up> <3 </3 <\\3 mañana vergüenza güenza création tonterías tonteréas <em class="grumpy">pain</em> <meta name="viewport" content="width=device-width"> <br />',
        "This is more like a Facebook message with a url: http://www.youtube.com/watch?v=dQw4w9WgXcQ, youtube.com google.com https://google.com/ ",
        "HTML entities &amp; other Web oddities can be an &aacute;cute <em class='grumpy'>pain</em> >:(",
        )

    if len(sys.argv) > 1 and (sys.argv[1]):
        samples = sys.argv[1:]

    for s in samples:
        print("======================================================================")
        print(s)
        tokenized = tok.tokenize(s)
        print("\n".join(tokenized).encode('utf8', 'ignore') if tok.use_unicode else "\n".join(tokenized))


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

# return True if keep, return False if filter out
def filterRTText(message):
    """
    """
    if message is None: return False
    rt, _ = rttext(message)
    return rt == ''

# return True if keep, return False if filter out
def filterRT(message):
    """
    """
    if message is None: return False
    return not message.startswith("RT ")

def replaceURL(message):
    message = re.sub(Url, "<URL>", message)
    return message

# return True if keep, return False if filter out
def filterURL(message):
    if message is None: return False
    # message, number_of_subs_made = re.subn(Url, "<URL>", message)
    # return number_of_subs_made == 0
    return not bool(re.search(Url, message))

def replaceUser(message):
    message = re.sub(r"@\w+", "<USER>", message)
    return message

def parsed_message(s):
    s = replaceUser(replaceURL(s))
    words = [word for word in tokenizer.tokenize(s) if ((word[0]!='#') and (word[0]!='@') and not Exclude_RE.search(word))]
    s = ' '.join(words).lower()
    if len(words)>=6:
        if 'YouTube' in words:
            s = ' '.join(words[0:5])
        else:
            s = ' '.join(words[0:6])
    return s

def _addTokenizerFile(sc):
    #sc.addPyFile(TOKENIZER_PATH) 
    sc.addPyFile('happierfuntokenizing.py') 
###############################
## Spark Portion:

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="Remove duplicate tweets within a group")
    parser.add_argument('--input', '--input_file', dest='input_file', default=DEF_INPUTFILE,
                help='')
    parser.add_argument('--output','--output_file', dest='output_file', default=DEF_OUTPUTFILE,
                help='')
    parser.add_argument('--message', '--message_field', dest='message_field', type=int, default=DEF_MSG_FIELD,
                help='')
    parser.add_argument('--group', '--group_field', dest='group_field', type=int, default=DEF_GROUP_FIELD,
                help='')
    parser.add_argument('--no_anonymize', action='store_false', dest='anonymize', default=DEF_ANNON,
                help='')
    args = parser.parse_args()

    if not (args.input_file and args.output_file):
        print("You must specify --input_file and --output_file")
        sys.exit()

    input_file, output_file, message_field, group_field, anonymize, header = args.input_file, args.output_file, args.message_field, args.group_field, args.anonymize, False

    session = SparkSession\
            .builder\
            .appName("deduplication")\
            .getOrCreate()
    sc = session.sparkContext

    # _addTokenizerFile(sc)
    # from happierfuntokenizing import Tokenizer
    tokenizer = Tokenizer(use_unicode=True)

    msgsDF = session.read.csv(input_file, header=header)
    msgsDF.sample(False, 0.01, seed=0).limit(5).show()
    print("Raw Data Length:",msgsDF.count())

    if not header:
        #dup_field = '_c' + str(max(message_field, group_field)+1)
        dup_field = 'temp_column'
        message_field = "_c" + str(message_field)
        group_field = "_c" + str(group_field)

    # does the deduplication
    dedupDF = msgsDF.withColumn(dup_field, udf(parsed_message, StringType())(message_field)).\
              dropDuplicates([group_field, dup_field]).drop(dup_field)
    print("Deduped Data Length:",dedupDF.count())
    #dedupDF.sample(False, 0.01, seed=0).limit(5).show()

    # Clear memory
    msgsDF.unpersist(blocking = True)
    
    # replaces URLs and USERs with constant text
    if anonymize:
        dedupDF = dedupDF.withColumn(message_field, udf(lambda s: replaceUser(replaceURL(s)), StringType())(message_field))

    # drop messages starting with "RT", modify rttext(message)
    filter_rt_udf = udf(filterRT, BooleanType())
    #dedupDF = dedupDF.filter(filter_rt_udf(message_field))
    #dedupDF = dedupDF.filter(dedupDF[message_field].startswith("RT "))

    # drop messages containing URLs, modify replaceURL()
    filter_url_udf = udf(filterURL, BooleanType())
    #dedupDF = dedupDF.filter(filter_url_udf(message_field))

    # Combine the two filters
    filteredDF = dedupDF.filter(filter_rt_udf(message_field) & filter_url_udf(message_field))
    print("Filtered Data Length:",filteredDF.count())
    #filteredDF.sample(False, 0.01, seed=0).limit(5).show()

    # Clear memory
    dedupDF.unpersist(blocking = True)
    
    #filteredDF.coalesce(1).write.csv(output_file)
    filteredDF.write.csv(output_file)