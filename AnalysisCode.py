import csv, os, os.path, sys
import ruffus
from functools import partial
import GeneralUtils
import PubmedUtils
import RunUtils



@ruffus.files('Data/QueryList.txt', 'Data/QueryList.sen')
def search_pubmed(ifile, ofile):
    
    with open(ifile) as handle:
        for row in csv.DictReader(handle):
            print 'searching', row['search']            
            id_list = PubmedUtils.SearchPUBMED(row['search'])
            fname = '%s--%s.res' % (GeneralUtils.slugify(row['org']), 
                                    GeneralUtils.slugify(row['search']))
            path = os.path.join('Data', 'SearchResults', fname)
            with open(path, 'w') as handle:
                for id_str in sorted(id_list):
                    handle.write(str(id_str)+'\n')


    GeneralUtils.touch(ofile)

@ruffus.files('Data/QueryList.sen', 'Data/PMC-ids.csv')
def get_PMCList(ifile, ofile):
    
    PubmedUtils.get_pmc_list('Data')


@ruffus.files(partial(RunUtils.FileIter, 'convert_pmids_to_pmcs'))
def convert_pmids_to_pmcs(ifiles, ofile):

    pmid2pmc = {}    
    with open(ifiles[1]) as handle:
        for row in csv.DictReader(handle):
            pmid2pmc[row['PMID']] = row['PMCID']
    
    present_ids = set()
    is os.path.exists(ofile):
        with open(ofile) as handle:
            for line in handle:
                present_ids.add(line.strip())

    new_ids = set()
    with open(ifiles[0]) as handle:
        for line in handle:
            idstr = line.strip()
            idstr = pmid2pmc.get(idstr, idstr)
            new_ids.add(idstr)

    write_ids = new_ids - present_ids
    if write_ids:
        with open(ifiles[0], 'w') as handle:
            for idstr in sorted(new_ids):
                handle.write(idstr + '\n')




@ruffus.follows(search_pubmed, get_PMCList)
def top_function():
    pass




if __name__ == '__main__':
    
    ruffus.pipeline_run([top_function])
