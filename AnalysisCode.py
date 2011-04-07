import csv, os, os.path, sys
import ruffus
from functools import partial
import GeneralUtils
import PubmedUtils
import RunUtils



@ruffus.files(partial(RunUtils.FileIter, 'search_pubmed'))
def search_pubmed(ifile, ofile, search_sent):
    
    id_list = PubmedUtils.SearchPUBMED(search_sent)
    with open(ofile, 'w') as handle:
        for id_str in sorted(id_list):
            handle.write(str(id_str)+'\n')


@ruffus.files('Data/QueryList.sen', 'Data/PMC-ids.csv')
def get_PMCList(ifile, ofile):
    
    PubmedUtils.get_pmc_list('Data')

@ruffus.follows(search_pubmed, get_PMCList)
@ruffus.files(partial(RunUtils.FileIter, 'convert_pmids_to_pmcs'))
def convert_pmids_to_pmcs(ifiles, ofile):

    pmid2pmc = {}    
    with open(ifiles[1]) as handle:
        for row in csv.DictReader(handle):
            pmid2pmc[row['PMID']] = row['PMCID']
    
    present_ids = set()
    if os.path.exists(ofile):
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
        with open(ofile, 'w') as handle:
            for idstr in sorted(new_ids):
                handle.write(idstr + '\n')




@ruffus.follows(search_pubmed, get_PMCList, convert_pmids_to_pmcs)
def top_function():
    pass




if __name__ == '__main__':
    
    ruffus.pipeline_run([top_function])
