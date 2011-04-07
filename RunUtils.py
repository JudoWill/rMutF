import os.path, os
import csv
from functools import partial
import GeneralUtils


def FileIter(func_name):

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


