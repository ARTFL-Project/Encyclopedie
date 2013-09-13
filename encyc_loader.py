import sys
import os
import errno
import philologic
from optparse import OptionParser
from glob import glob
from philologic.LoadFilters import *
from philologic.PostFilters import *
from philologic.Parser import Parser
from philologic.ParserHelpers import *
from philologic.Loader import Loader

def normalize_divs(*columns):
    def normalize_these_columns(loader_obj,text,depth=5):
        current_values = {}
        tmp_file = open(text["sortedtoms"] + ".tmp","w")
        for column in columns:
            current_values[column] = ""
        for line in open(text["sortedtoms"]):
            type, word, id, attrib = line.split('\t')
            id = id.split()
            record = Record(type, word, id)
            record.attrib = eval(attrib)        
            if type == "div1":
                for column in columns:
                    if column in record.attrib:
                        current_values[column] = record.attrib[column]
                    else:
                        current_values[column] = ""
            elif type == "div2":
                for column in columns:
                    if column in record.attrib:
                        current_values[column] = record.attrib[column]
            elif type == "div3":
                for column in columns:
                    if column not in record.attrib:
                        record.attrib[column] = current_values[column]
            print >> tmp_file, record
        tmp_file.close()
        os.remove(text["sortedtoms"])
        os.rename(text["sortedtoms"] + ".tmp",text["sortedtoms"])
    return normalize_these_columns

def normalize_columns_post(*columns):
    def normalize_these_columns_post(loader):
        for k,v in loader.metadata_types.items():
            if k in columns:
                loader.metadata_types[k] = "div3"
    return normalize_these_columns_post
    
def normalize_unicode_raw_words(loader_obj, text):
    tmp_file = open(text["raw"] + ".tmp","w")
    for line in open(text["raw"]):
        rec_type, word, id, attrib = line.split('\t')
        id = id.split()
        if rec_type == "word":
            word = word.decode("utf-8").lower().encode("utf-8")
        record = Record(rec_type, word, id)
        record.attrib = eval(attrib)
        print >> tmp_file, record
    os.remove(text["raw"])
    os.rename(text["raw"] + ".tmp",text["raw"])   

def make_sorted_toms(*types):
    def sorted_toms(loader_obj, text):
        type_pattern = "|".join("^%s" % t for t in types)
        tomscommand = "cat %s | egrep \"%s\" | sort %s > %s" % (text["raw"],type_pattern,loader_obj.sort_by_id,text["sortedtoms"])
        os.system(tomscommand)    
    return sorted_toms

#########################
## Command-line parsing #
#########################
usage = "usage: %prog [options] database_name files"
parser = OptionParser(usage=usage)
parser.add_option("-q", "--quiet", action="store_true", dest="quiet", help="suppress all output")
parser.add_option("-l", "--log", default=False, dest="log", help="enable logging and specify file path")
parser.add_option("-c", "--cores", type="int", default="2", dest="workers", help="define the number of cores for parsing")
parser.add_option("-t", "--templates", default=False, dest="template_dir", help="define the path for the templates you want to use")
parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="add debugging to your load")
parser.add_option("--no-template", action="store_true", default=False, dest="no_template", help="build a database without templates for HTML rendering")


##########################
## System Configuration **
##########################

# Set the filesytem path to the root web directory for your PhiloLogic install.
database_root = '/var/www/html/philo4/'
# /var/www/html/philologic/ is conventional for linux,
# /Library/WebServer/Documents/philologic for Mac OS.
# Please follow the instructions in INSTALLING before use.

# Set the URL path to the same root directory for your philologic install.
url_root = 'http://pantagruel.ci.uchicago.edu/philo4/'
# http://localhost/philologic/ is appropriate if you don't have a DNS hostname.

if database_root is None or url_root is None:
    print >> sys.stderr, "Please configure the loader script before use.  See INSTALLING in your PhiloLogic distribution."
    exit()

template_dir = database_root + "_system_dir/_install_dir/"
# The load process will fail if you haven't set up the template_dir at the correct location.


###########################
## Configuration options ##
###########################

## Parse command-line arguments
(options, args) = parser.parse_args(sys.argv[1:])
try:
    dbname = args[0]
    args.pop(0)
    files = args[:]
#    if args[-1].endswith('/') or os.path.isdir(args[-1]):   
#        files = glob(args[-1] + '/*')
#    else:
#        files = args[:]
except IndexError:
    print >> sys.stderr, "\nError: you did not supply a database name or a path for your file(s) to be loaded\n"
    parser.print_help()
    sys.exit()
## Number of cores used for parsing: you can define your own value on the
## command-line, stay with the default, or define your own value here
workers = options.workers or 2

## This defines which set of templates to use with your database: you can stay with
## the default or speicify another path from the command-line. Alternatively, you
## can edit it here.
if not options.no_template:
    template_dir = options.template_dir or template_dir
else:
    template_dir = False
## Define the type of output you want. By default, you get console output for your database
## load. You can however set a quiet option on the command-line, or set console_output
## to False here.
console_output = True
if options.quiet:
    console_output = False
    
## Define a path for a log of your database load. This option can be defined on the command-line
## or here. It's disabled by default.
log = options.log or False

## Set debugging if you want to keep all the parsing data, as well as debug the templates
debug = options.debug or False

# Define text objects for ranked relevancy: by default it's ['doc']. Disable by supplying empty list
default_object_level = 'div3' 

# Data tables to store.
tables = ['toms', 'pages', 'ranked_relevance']

# Define filters as a list of functions to call, either those in Loader or outside
filters = [normalize_unicode_raw_words,make_word_counts, generate_words_sorted,make_token_counts,make_sorted_toms("doc","div1","div2","div3"),
                prev_next_obj, normalize_divs("head","articleAuthor","normalizedClass","volume","pos"),
                word_frequencies_per_obj(),generate_pages, make_max_id]  

post_filters = [normalize_columns_post("articleAuthor","normalizedClass","head","volume","pos"),word_frequencies,
                normalized_word_frequencies,metadata_frequencies,normalized_metadata_frequencies,metadata_relevance_table]

# Define text objects to generate plain text files for various machine learning tasks
plain_text_obj = []
if plain_text_obj:
    filters.extend([store_in_plain_text(*plaint_text_obj)])

extra_locals = {"db_url": url_root + dbname}

## Define which search reports to enable
## Note that this can still be configured in your database db_locals.py file
search_reports = ['concordance', 'kwic', 'relevance', 'collocation', 'time_series']
extra_locals['search_reports'] = search_reports

###########################
## Set-up database load ###
###########################

Philo_Types = ["doc","div"] # every object type you'll be indexing.  pages don't count, yet.

XPaths =  [("doc","."),("div",".//div1"),("div",".//div2"),("page",".//pb")]         

Metadata_XPaths = [ # metadata per type.  '.' is in this case the base element for the type, as specified in XPaths above.
    # MUST MUST MUST BE SPECIFIED IN OUTER TO INNER ORDER--DOC FIRST, WORD LAST
    ("doc","./teiheader//titlestmt/title","title"),
    ("doc","./teiheader//titlestmt/author","author"),
    ("doc","./teiheader//profiledesc/creation/date","date"),
    ("div","./index[@type='headword']@value","head"),
    ("div","./index[@type='author']@value","articleAuthor"),
    ("div","./index[@type='objecttype']@value","articleType"),
    ("div","./index[@type='class']@value","class"),
    ("div","./index[@type='normclass']@value","normalizedClass"),
    ("div","./index[@type='englishclass']@value","englishClass"),
    ("div","./index[@type='generatedclass']@value","generatedClass"),
    ("div","./index[@type='pos']@value","pos"),
    ("div",".@n","n"),
    ("div",".@id","id"),
    ("div",".@vol","volume"),
    ("page",".@n","n"),
    ("page",".@fac","img")
]
           #  "doc" : [(ContentExtractor,"./teiHeader/fileDesc/titleStmt/author","author"),
           #           (ContentExtractor,"./teiHeader/fileDesc/titleStmt/title", "title"),
           #           (ContentExtractor,"./teiHeader/sourceDesc/biblFull/publicationStmt/date", "date"),
           #           (AttributeExtractor,"./text/body/volume@n","volume"),
           #           (AttributeExtractor,".@xml:id","id")],
           #  "div" : [(ContentExtractor,"./head","head"),
           #           (ContentExtractor,"./head//*","head"),
           #           (AttributeExtractor,".@n","n"),
           #           (AttributeExtractor,".@xml:id","id")],
           #  "para": [(ContentExtractor,"./speaker", "who"),
           #           (ContentExtractor,"./head","head")],
           #  "word": [(AttributeExtractor,".@lemma","lemma"),
           #           (ContentExtractor,".","token"),
           #           (AttributeExtractor,".@ana","ana")],
           #  "page": [(AttributeExtractor,".@n","n"),
           #           (AttributeExtractor,".@src","img")],
           #}

pseudo_empty_tags = ["milestone"]
suppress_tags = ["teiheader",".//head"]
word_regex = r"([\w]+)"
punct_regex = r"([\.?!])"

token_regex = word_regex + "|" + punct_regex 
extra_locals["word_regex"] = word_regex
extra_locals["punct_regex"] = punct_regex

#############################
# Actual work.  Don't edit. #
#############################

os.environ["LC_ALL"] = "C" # Exceedingly important to get uniform sort order.
os.environ["PYTHONIOENCODING"] = "utf-8"
    
db_destination = database_root + dbname
data_destination = db_destination + "/data"
db_url = url_root + "/" + dbname

try:
    os.mkdir(db_destination)
except OSError:
    print "The %s database already exists" % dbname
    print "Do you want to delete this database? Yes/No"
    choice = raw_input().lower()
    if choice.startswith('y'):
        os.system('rm -rf %s' % db_destination)
        os.mkdir(db_destination)
    else:
        sys.exit()

if template_dir:
    os.system("cp -r %s* %s" % (template_dir,db_destination))
    os.system("cp %s.htaccess %s" % (template_dir,db_destination))


####################
## Load the files ##
####################

l = Loader(data_destination,
           token_regex,
           XPaths,
           Metadata_XPaths,
           filters, 
           pseudo_empty_tags,
           suppress_tags,
           default_object_level=default_object_level,
           debug=debug)

#destination,token_regex=default_token_regex,xpaths=default_xpaths,
#                 metadata_xpaths=default_metadata,filters=default_filters,
#                 pseudo_empty_tags=[],suppress_tags=[],console_output=True,
#                 log=False, debug=False)

l.add_files(files)
filenames = l.list_files()
print filenames
load_metadata = [{"filename":f} for f in sorted(filenames)]
l.parse_files(workers,load_metadata)
l.merge_objects()
l.analyze()
l.make_tables(tables)
l.finish(post_filters,**extra_locals)

print "\nDone indexing."
print "Your database is viewable at " + db_url + "\n"
