import os.path, os
import csv
from functools import partial
import GeneralUtils
from mutation_finder import mutation_finder_from_regex_filepath as mutfinder_gen

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

    elif func_name == 'download_pmids':
        
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
        finder = None#mutfinder_gen('regex.txt')

        files = sorted([x for x in os.listdir(sdir('')) if x.endswith('.sent')])

        for f in files:
            name = f.split('.')[0]
            yield sdir(f), odir(name + '.mut')
        
    elif func_name == 'process_mut_file':
        
        sdir = partial(os.path.join, 'Data', 'MutFiles')
        odir = partial(os.path.join, 'Data', 'ProteinFiles')

        files = sorted([x for x in os.listdir(sdir('')) if x.endswith('.mut')])

        for f in files:
            name = f.split('.')[0]
            yield sdir(f), (odir(name + '.prot'), odir(name + '.sen'))
    elif func_name == 'mapping_files':
        path = 'Data/Mapping/'
        items = (('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping.dat.gz', 'idmapping.dat.sort'),
                    ('ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz', 'gene_info'),
                    ('ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz', 'PMC-ids.csv'),
                    ('ftp://nlmpubs.nlm.nih.gov/online/mesh/.asciimesh/d2011.bin', 'desc2011.bin'))
        for url, ofile in items:
            yield None, os.path.join(path, ofile), url, path
