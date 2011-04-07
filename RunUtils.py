import os.path, os
import partial



def FileIter(func_name):

    if func_name == 'convert_pmids_to_pmcs':
        sdir = partial(os.path.join('Data', 'SearchResults'))
        pmc_file = os.path.join('Data', 'PMC-ids.csv')
        files = [x for x in os.listdir(sdir) if x.endswith('.res')]
        for f in files:
            yield (sdir(f), pmc_file), sdir(f+'.conv')
