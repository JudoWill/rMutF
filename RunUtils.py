import os.path, os
import csv
from functools import partial
import GeneralUtils


def FileIter(func_name):
    """A general iterator for all of the ruffus functions in the pipeline."""
    
    if func_name == 'convert_pmids_to_pmcs':
        sdir = partial(os.path.join,'Data', 'SearchResults')
        pmc_file = os.path.join('Data', 'PMC-ids.csv')
        files = [x for x in os.listdir(sdir('')) if x.endswith('.res')]
        for f in files:
            yield (sdir(f), pmc_file), sdir(f+'.conv')

    elif func_name == 'search_pubmed':
        sdir = partial(os.path.join,'Data', 'SearchResults')
        queryfile = os.path.join('Data', 'QueryList.txt')
        with open(queryfile) as handle:
            for row in csv.DictReader(handle):
                fname = '%s--%s.res' % (GeneralUtils.slugify(row['org']), 
                                        GeneralUtils.slugify(row['search']))
                ofile = sdir(fname)
                yield queryfile, ofile, row['search']

    elif func_name == 'download_pmids'
        
        sdir = partial(os.path.join,'Data', 'SearchResults')
        odir = os.path.join('Data', 'RawXML')
        files = [x for x in os.listdir(sdir('')) if x.endswith('.conv')]
        
        for f in files:
            yield sdir(f), sdir(f+'.dl'), odir

    elif func_name == 'extract_text':
        
        sdir = partial(os.path.join, 'Data', 'RawXML')
        odir = partial(os.path.join, 'Data', 'SentenceFiles')

        files = sorted([x for x in os.listdir(sdir('')) if x.endswith('.xml')])
        for f in files:
            name = f.split('.')[0]
            if f.startswith('PMC'):
                typ = 'pmc'
            else:
                typ = 'pubmed'

            yield sdir(f), odir(name+'.sent'), typ

    elif func_name == 'get_mutations':

        sdir = partial(os.path.join, 'Data', 'SentenceFiles')
        odir = partial(os.path.join, 'Data', 'MutFiles')

        files = sorted([x for x in os.listdir(sdir('')) if x.endswith('.sent')])

        for f in files:
            name = f.split('.')[0]
            yield sdir(f), odir(name + '.mut')            
        
        

