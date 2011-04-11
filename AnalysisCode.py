import csv, os, os.path, sys
import ruffus
from functools import partial
from itertools import izip
import GeneralUtils
import PubmedUtils
import RunUtils
import WhatizitUtils


@ruffus.jobs_limit(1)
@ruffus.files(partial(RunUtils.FileIter, 'search_pubmed'))
def search_pubmed(ifile, ofile, search_sent):
    """Uses the EUtils pipeline to search all data in the input file.
    
    Arguements:
    ifile -- The search file (not actually used but needed by Ruffus 
                for dependancy management)
    ofile -- The result file
    search_sent -- The sentence to search by EUtils
    """
    
    id_list = PubmedUtils.SearchPUBMED(search_sent)
    with open(ofile, 'w') as handle:
        for id_str in sorted(id_list):
            handle.write(str(id_str)+'\n')


@ruffus.jobs_limit(1)
@ruffus.files('Data/QueryList.sen', 'Data/PMC-ids.csv')
def get_PMCList(ifile, ofile):
    """Downloads the PMC-ids.csv file which maps PMIDS to PMCIDS.
    
    Arguements:
    ifile -- The sentinal file.
    ofile -- The downloaded PMC-ids mapping file.
    """
    
    PubmedUtils.get_pmc_list('Data')

@ruffus.jobs_limit(1)
@ruffus.follows(search_pubmed, get_PMCList)
@ruffus.files(partial(RunUtils.FileIter, 'convert_pmids_to_pmcs'))
def convert_pmids_to_pmcs(ifiles, ofile):
    """Converts PMID to PMCIDS and makes sure to only add NEW ids.
    
    Arguements:
    ifiles -- A 2-tuple (search-results, PMC-ids-mapping)
    ofile -- The mapped results file.
    """

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

@ruffus.jobs_limit(1)
@ruffus.follows(convert_pmids_to_pmcs)
@ruffus.files(partial(RunUtils.FileIter, 'download_pmids'))
def download_pmids(ifile, ofile, odir):
    """Downloads the raw Pubmed XML data."""
   
    needed_ids = set()
    with open(ifile) as handle:
        for line in handle:
            needed_ids.add(line.strip())

    present_ids = set()
    for f in (x for x in os.listdir(odir) if x.endswith('.xml')):
        present_ids.add(f.split('.')[0])

    name_fun = partial(os.path.join, odir)
    needed_ids -= present_ids
    pmids = (x for x in needed_ids if not x.startswith('PMC'))
    pmcs = (x for x in needed_ids if x.startswith('PMC'))
    for xml, pmid in PubmedUtils.GetXMLfromList(pmids, db = 'pubmed'):
        with open(name_fun(pmid+'.xml'), 'w') as handle:
            handle.write(xml)
    for xml, pmid in PubmedUtils.GetXMLfromList(pmids, db = 'pmc'):
        with open(name_fun(pmid+'.xml'), 'w') as handle:
            handle.write(xml)
            
    GeneralUtils.touch(ofile)

@ruffus.follows(download_pmids)
@ruffus.files(partial(RunUtils.FileIter, 'extract_text'))
def extract_text(ifile, ofile, typ):
    """Extracts raw-text paragraphs from PMC and PMID xml files."""
    
    if typ == 'pmc':
        iterable = PubmedUtils.ExtractPMCPar(open(ifile).read())
    elif typ == 'pubmed':
        iterable = PubmedUtils.ExtractPubPar(open(ifile).read())

    with open(ofile, 'w') as handle:
        for ind, par in enumerate(iterable):
            handle.write('%i\t%s\n' % (ind, par))
        
@ruffus.jobs_limit(1)
@ruffus.follows(extract_text)
@ruffus.files(partial(RunUtils.FileIter, 'get_mutations'))
def get_mutations(ifile, ofile):
    """Uses a REGEXP to find mutagenesis experiments."""
    
    PubmedUtils.process_mutation(ifile, ofile)


@ruffus.follows(get_mutations)
@ruffus.files(partial(RunUtils.FileIter, 'process_mut_file'))
def process_mut_file(ifile, ofiles):
    """Processes mut files and checks them for mentions of protein-names"""
    
    with open(ifile) as handle:
        rows = list(csv.DictReader(handle, delimiter = '\t'))
    if rows:
        ofields = ('ParNum', 'SentNum', 'Mutation', 'Swissprot', 'Text')
        writer = csv.DictWriter(open(ofiles[0], 'w'), ofields, delimiter = '\t')
        writer.writerow(dict(zip(ofields, ofields)))
        sent_list = [x['Text'] for x in rows]
        iterable = WhatizitUtils.ask_whatizit(sent_list, 
                            pipeline = 'whatizitSwissprot')
        for row, reslist in izip(rows, iterable):
            for res in reslist:
                row['Swissprot'] = res
                writer.writerow(row)
        GeneralUtils.touch(ofiles[1])
    else:
        for f in ofiles:
            GeneralUtils.touch(f)

@ruffus.follows(process_mut_file)
@ruffus.merge('Data/ProteinFiles/*.prot', ('Data/Mergedresults.txt', 'Data/Mergedresults.sen'))
def merge_results(ifiles, ofiles):
    """Merges the results of the protein-name recognition into one file."""

    with open(ofiles[0], 'w') as ohandle:
        ofields = ('Article', 'ParNum', 'SentNum', 'Mutation', 'Swissprot')
        writer = csv.DictWriter(ohandle, ofields, delimiter = '\t',
                                    extrasaction = 'ignore')
        writer.writerow(dict(zip(ofields, ofields)))
        for f in ifiles:
            with open(f) as handle:
                rows = list(csv.DictReader(handle, delimiter = '\t'))
            art = f.split(os.sep)[-1].split('.')[0]
            for row in rows:
                row['Article'] = art
                writer.writerow(row)
    
    GeneralUtils.touch(ofiles[1])    



@ruffus.follows(merge_results)
def top_function():
    pass




if __name__ == '__main__':
    
    ruffus.pipeline_run([top_function], multiprocess = 4)
