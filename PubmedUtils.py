import sys
import os.path
import shlex
import csv
from ensure_ascii import unicode_to_ascii
import re, urllib2
from datetime import datetime
from BeautifulSoup import BeautifulStoneSoup
from itertools import islice
from GeneralUtils import TimedSemaphore, pushd
from subprocess import check_call
from nltk.tokenize import sent_tokenize
from mutation_finder import mutation_finder_from_regex_filepath as mutfinder_gen

def take(NUM, iterable):
    return list(islice(iterable, NUM))



def GetXML(ID_LIST, db = 'pubmed'):

    valid_db = set(['pubmed', 'pmc'])
    assert db in valid_db

    POST_URL = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/epost.fcgi?db=%s' % db
    RET_URL = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=%s&query_key=1&mode=xml&rettype=full' % db

    pmid_list = ','.join(map(lambda x: str(x), ID_LIST))
    post_req_url = POST_URL + '&id=' + pmid_list

    post_res = urllib2.urlopen(post_req_url).read()

    web_env = re.findall('<WebEnv>(.*?)</WebEnv>', post_res)[0]

    req_url = RET_URL + '&WebENV=' + web_env

    xml_data = urllib2.urlopen(req_url).read()
    return xml_data.decode('ascii', 'ignore')


def GetXMLfromList(IDS, db = 'pubmed', NUM_TAKE = 50, WAITINGSEM = TimedSemaphore(2,3)):

    def GetPubmedTuple(article_set):

        soup = BeautifulStoneSoup(article_set)
        for art in soup.findAll('pubmedarticle'):
            yield art.prettify(), art.find('pmid').string

    def GetPMCTuple(article_set):
        soup = BeautifulStoneSoup(article_set)
        for art in soup.findAll('article'):
            found_id = None
            for id in art.findAll('article-id'):
                if 'pmc' in str(id):
                    found_id = id.string
                    yield art.prettify(), 'PMC'+found_id
                    break
            if found_id is None:
                raise KeyError, 'Could not find "pmc"'
                    

    valid_db = set(['pubmed', 'pmc'])
    assert db in valid_db


    if db == 'pubmed':
        data_getter = GetPubmedTuple
    else:
        data_getter = GetPMCTuple


    IDS = list(IDS) #since we need to traverse this a few times we need to make sure it doesn't get exhausted

    objiter = iter(IDS)
    block = take(NUM_TAKE, objiter)
    counter = NUM_TAKE

    while len(block) != 0:
        with WAITINGSEM:
            data = GetXML(block, db = db)

        for art, id in data_getter(data):
            yield art, id
        block = take(NUM_TAKE, objiter)
        print 'retrieved %i of %i articles' % (counter, len(IDS))
        counter += NUM_TAKE



def SearchPUBMED(search_sent, recent_date = None, BLOCK_SIZE = 100000, START = 0):

    POST_URL = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&'
    POST_URL += 'retmax=%i&' % BLOCK_SIZE
    if START > 0:
        POST_URL += 'retstart=%i&' % START

    search_term = search_sent.replace(' ', '%20')
    search_term = search_term.replace('-', '%20')
    search_term = search_term.replace('+', '%20')
    search_url = POST_URL + '&term=' + search_term
    if recent_date:
        time_delta = datetime.today()-recent_date
        search_url += '&reldate=' + str(time_delta.days)
    
    xml_data = urllib2.urlopen(search_url).read()

    id_list = re.findall('<Id>(\d*)</Id>', xml_data)
    id_nums = map(lambda x: int(x), id_list)

    if len(id_nums) == BLOCK_SIZE:
        return id_nums + SearchPUBMED(search_sent, recent_date = recent_date,
                                      BLOCK_SIZE = BLOCK_SIZE, START = START+BLOCK_SIZE-1)
    else:
        return id_nums



def ExtractPMCPar(xmldata):
    """Yields sucessive paragraphs from a PMC xml"""

    xmltree = BeautifulStoneSoup(xmldata)
    for par in xmltree.findAll('p'):
        buf = ''
        for item in par.findAll(text=True):
            buf += item.string.strip()

        yield buf


def ExtractPubPar(xmldata):
    """Yields sucessive paragraphs from a Pubmed xml"""

    xmltree = BeautifulStoneSoup(xmldata)
    v = xmltree.find('abstracttext')
    if v:
        yield v.string.strip()

def process_mutation(ifile, ofile, finder = None):
    
    with open(ifile) as handle:
        reader = csv.DictReader(handle, delimiter = '\t', fieldnames = ('ParNum', 'Text'))
        rows = [x for x in reader]

    if finder is None:
        finder = mutfinder_gen('regex.txt')

    ofields = ('ParNum', 'SentNum', 'Mutation', 'Text')
    with open(ofile, 'w') as handle:
        writer = csv.DictWriter(handle, ofields, delimiter = '\t')
        writer.writerow(dict(zip(ofields, ofields)))
        for row in rows:
            if row['Text']:
                sent_list = ['']+list(sent_tokenize(row['Text'].replace('\n', '')))+['']

                for sentnum, sent in enumerate(sent_list):
                    for mut, _ in finder(sent).items():
                        text = ' '.join(sent_list[sentnum-1:sentnum+1])
                        nrow = {'Text': text,
                                'ParNum': row['ParNum'],
                                'SentNum': sentnum,
                                'Mutation': mut}
                        writer.writerow(nrow)

def process_many_mutation(ifiles, ofiles):
    """Process Sentence files in Batch format.
     This function is useful for when you need to procecess lots of files in 
    one large batch.
    """
    
    finder = mutfinder_gen('regex.txt')

    for ifile, ofile in zip(ifiles, ofiles):
        print ifile
        with open(ifile) as handle:
            reader = csv.DictReader(handle, delimiter = '\t', fieldnames = ('ParNum', 'Text'))
            rows = [x for x in reader]
            
        ofields = ('ParNum', 'SentNum', 'Mutation', 'Text')
        with open(ofile, 'w') as handle:
            writer = csv.DictWriter(handle, ofields, delimiter = '\t')
            writer.writerow(dict(zip(ofields, ofields)))
            for row in rows:
                if row['Text']:
                    sent_list = ['']+list(sent_tokenize(row['Text'].replace('\n', '')))+['']

                    for sentnum, sent in enumerate(sent_list):
                        for mut, _ in finder(sent).items():
                            text = ' '.join(sent_list[sentnum-1:sentnum+1])
                            nrow = {'Text': text,
                                    'ParNum': row['ParNum'],
                                    'SentNum': sentnum,
                                    'Mutation': mut}
                            writer.writerow(nrow)

def get_pmc_list(path):
    """Retrieves the PMC id list and unzips it to the specified path"""

    
    with pushd(path):
        cmd = shlex.split('wget ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz')
        check_call(cmd)
        cmd = shlex.split('gzip -d PMC-ids.csv.gz')
        check_call(cmd)



















