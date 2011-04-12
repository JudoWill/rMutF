from subprocess import check_call
from GeneralUtils import pushd, touch
from itertools import groupby, chain, dropwhile
from collections import defaultdict
from functools import partial
from operator import itemgetter, methodcaller, not_
import shlex
import csv

def get_uniprot_mapping(path):
    """Retrieves the PMC id list and unzips it to the specified path"""

    
    with pushd(path):
        cmd = shlex.split('wget -N ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping.dat.gz')
        check_call(cmd)
        cmd = shlex.split('gzip -d idmapping.dat.gz')
        check_call(cmd)
        cmd = shlex.split('wget -N ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz')
        check_call(cmd)
        cmd = shlex.split('gzip -d gene_info.gz')
        check_call(cmd)        
        touch('idmapping.dat')
        touch('gene_info')





def uniprot_to_entrez(uniprot_ids):
    """Converts uniprot ids to entrez gene-ids. Returns a dict. of sets"""

    res = defaultdict(set)
    all_ids = sorted(set(uniprot_ids))
    fields = ('ID', 'db', 'value')
    with open('Data/Mapping/idmapping.dat.sort') as handle:
        ihandle = dropwhile(lambda x: not x.startswith(all_ids[0]), handle)
        ihandle.next()
        print 'actually reading now'
        iterable = csv.DictReader(handle, fieldnames = fields, delimiter = '\t')
        for idval in all_ids:
            #print idval
            niter = dropwhile(lambda x: x['ID'] < idval, iterable)
            for row in niter:
                if row['ID'] != idval:
                    break
                if row['db'] == 'GeneID':
                    res[idval].add(row['value'])
    return res



def entrez_to_genesymbol(entrez_ids):
    """Converts a list of entrez ids into gene-symbols. Returns a dict."""

    res = {}
    all_ids = set(entrez_ids)
    fields = ('tax_id', 'GeneID', 'Symbol', 'LocusTag', 'Synonyms', 'dbXrefs', 'chromosome', 
                'map_location', 'type_of_gene', 'Symbol_from_nomenclature_authority', 
                'Full_name_from_nomenclature_authority', 'Nomenclature_status', 
                'Other_designations', 'Modification_date')
    with open('Data/Mapping/gene_info') as handle:
        handle.next()
        iterable = csv.DictReader(handle, fieldnames = fields, delimiter = '\t')
        for row in iterable:
            if row['GeneID'] in all_ids:
                res[row['GeneID']] = row['Symbol']

    return res


def uniprot_to_symbol(uniprot_ids, uniprot2entrez = None):

    if not uniprot2entrez:
        uniprot2entrez = uniprot_to_entrez(uniprot_ids)
    entrezids = set(chain.from_iterable(uniprot2entrez.values()))
    entrez2symbol = entrez_to_genesymbol(entrezids)

    res = defaultdict(set)
    for uniprot, ids in uniprot2entrez.iteritems():
        for entrez in ids:
            sym = entrez2symbol[entrez]
            res[uniprot].add(sym)

    return res
    







