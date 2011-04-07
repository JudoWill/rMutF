import csv, os, os.path, sys
import ruffus
from functools import partial
import GeneralUtils
import PubmedUtils




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



@ruffus.follows(search_pubmed, get_PMCList)
def top_function():
    pass




if __name__ == '__main__':
    
    ruffus.pipeline_run([top_function])
