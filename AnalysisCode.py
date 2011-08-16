import csv, os, os.path, sys
import ruffus
import argparse
from functools import partial
from itertools import izip, chain, ifilter, groupby
from operator import itemgetter
import GeneralUtils
import PubmedUtils
import RunUtils
import WhatizitUtils
import UniprotUtils
import MeshUtils
from HTMLParser import HTMLParseError
from collections import defaultdict


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
@ruffus.files(partial(RunUtils.FileIter, 'mapping_files'))
def download_files(ifile, ofile, url, path):
    """Downloads the mapping files needed for various steps"""
    
    GeneralUtils.download_file(path, url, sort = ofile.endswith('.sort'))
    GeneralUtils.touch(ofile)

@ruffus.jobs_limit(1)
@ruffus.files('Mapping/d2011.bin', 'Mapping/MeshMapping.txt')
@ruffus.follows(download_files)
def convert_mesh_mapping(ifile, ofile):
    
    MeshUtils.convert_mesh_mapping(ifile, ofile)

@ruffus.jobs_limit(1)
@ruffus.follows(search_pubmed)
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
    """Downloads the raw Pubmed XML data.

    Arguements:
    ifile -- The input file in which each line contains a SINLGE PMID/PMCID to download.
    ofile -- The sentinal file to touch when finished
    odir -- The output directoy to download all XML files into.
    """
   
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
    """Extracts raw-text paragraphs from PMC and PMID xml files.

    Arguments:
    ifile -- An input xml file to extract raw text from.
    ofile -- The output file to write data into.
    typ -- A string indicating the type of xml file (pmc or pubmed)
    """
    
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
    """Uses a REGEXP to find mutagenesis experiments.

    Arguments:
    ifile -- The input raw text file to search
    ofile -- The output file to put data into
    """
    
    PubmedUtils.process_mutation(ifile, ofile)

@ruffus.follows(get_mutations)
@ruffus.files(partial(RunUtils.FileIter, 'process_mut_file'))
def process_mut_file(ifile, ofiles):
    """Processes mut files and checks them for mentions of protein-names.

    Arguments:
    ifile -- The input Mutation file with text and mutation mentions.
    ofiles -- A 2-tuple (result-file, sentinal-file)
    """
    
    with open(ifile) as handle:
        rows = list(csv.DictReader(handle, delimiter = '\t'))
    if rows:
        ofields = ('ParNum', 'SentNum', 'Mutation', 'Swissprot', 'ProtText', 'Text', 'Mesh')
        writer = csv.DictWriter(open(ofiles[0], 'w'), ofields, delimiter = '\t')
        writer.writerow(dict(zip(ofields, ofields)))
        sent_list = [x['Text'] for x in rows]

        swiss_it = WhatizitUtils.ask_whatizit(sent_list, 
                            pipeline = 'whatizitSwissprot')
        mesh_it = WhatizitUtils.ask_whatizit(sent_list, 
                            pipeline = 'whatizitMeshUp')
        try:
            for row, swiss_group, mesh_group in izip(rows, swiss_it, mesh_it):
                if swiss_group:
                    meshterms = '|'.join(x[1] for x in mesh_group)
                    for prot_text, reslist in swiss_group:
                        for res in reslist:
                            row['Swissprot'] = res
                            row['ProtText'] = prot_text
                            row['Mesh'] = meshterms
                            writer.writerow(row)
        except HTMLParseError:
            pass
        GeneralUtils.touch(ofiles[1])
    else:
        for f in ofiles:
            GeneralUtils.touch(f)

@ruffus.follows(process_mut_file)
@ruffus.merge('Data/ProteinFiles/*.prot', ('Data/Mergedresults.txt', 'Data/Mergedresults.sen'))
def merge_results(ifiles, ofiles):
    """Merges the results of the protein-name recognition into one file.

    Arguments:
    ifiles -- A list of ALL results from process_mut_file to aggregate together.
    ofiles -- A 2-tuple (merged-file, sentinal-file)
    """

    with open(ofiles[0], 'w') as ohandle:
        ofields = ('Article', 'ParNum', 'SentNum', 'Mutation', 'Swissprot', 'ProtText', 'Mesh')
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

@ruffus.files(('Data/Mergedresults.txt', 'Data/Mapping/idmapping.dat.sort', 'Data/Mapping/gene_info'), 
                'Data/Convertedresults.txt')
@ruffus.follows(merge_results)
def convert_results(ifiles, ofile):
    
    with open(ifiles[0]) as handle:
        uniprot_ids = [x['Swissprot'] for x in csv.DictReader(handle, delimiter = '\t')]
    uniprot2entrez = UniprotUtils.uniprot_to_entrez(uniprot_ids)    
    print 'got entrez mapping', len(uniprot2entrez)    
    uniprot2symbol = UniprotUtils.uniprot_to_symbol(uniprot_ids, 
                                                    uniprot2entrez = uniprot2entrez, 
                                                    with_taxid = True)
    print 'got symbol mapping', len(uniprot2symbol)

    convfields = ('Article', 'ParNum', 'SentNum', 'Mutation', 'Swissprot', 'ProtText', 'GeneID', 'Symbol', 'Taxid')
    with open(ofile, 'w') as conv_handle:
        conv_writer = csv.DictWriter(conv_handle, convfields, delimiter = '\t', extrasaction = 'ignore')
        conv_writer.writerow(dict(zip(convfields, convfields)))
        with open(ifiles[0]) as ihandle:
            for row in csv.DictReader(ihandle, delimiter = '\t'):
                for geneid, genesym in zip(uniprot2entrez[row['Swissprot']], uniprot2symbol[row['Swissprot']]):
                    row['GeneID'] = geneid
                    row['Symbol'] = genesym[0]
                    row['Taxid'] = genesym[1]
                    conv_writer.writerow(row)


@ruffus.files(('Data/Convertedresults.txt', 'allowed_taxids.txt'), 
                'Data/AggregatedResults.txt')
@ruffus.follows(convert_results)
def aggregate_results(ifiles, ofile):

    with open(ifiles[1]) as handle:
        tax2org = dict([(x['taxid'], x['name']) for x in csv.DictReader(handle)])
    
    mut_dict = defaultdict(set)
    with open(ifiles[0]) as handle:
        iterable = csv.DictReader(handle, delimiter = '\t')
        taxiter = ifilter(lambda x:x['Taxid'] in tax2org, iterable)
        grouper = itemgetter('Article', 'ParNum', 'SentNum', 'Mutation', 'ProtText')
        for key, rows in groupby(taxiter, grouper):
            rows = list(rows)
            uni_orgs = set((tax2org[x['Taxid']] for x in rows))
            uni_prots = set((x['Symbol'] for x in rows))
            if len(uni_orgs) == 1:
                uni_org = uni_orgs.pop()
                for prot in uni_prots:
                    mut_dict[(uni_org, prot, key[3], key[4])].add(key[0])
            

    aggfields = ('Organism', 'Symbol', 'Mutation', 'ProtText', '#articles', 'Articles')
    with open(ofile, 'w') as agg_handle:                    
        agg_writer = csv.DictWriter(agg_handle, aggfields, delimiter = '\t', extrasaction = 'ignore')
        agg_writer.writerow(dict(zip(aggfields, aggfields)))
        for (org, genesym, mut, prot_text), arts in sorted(mut_dict.iteritems()):
            agg_writer.writerow({'Organism':org,'Symbol':genesym, 'Mutation': mut, 
                                '#articles':len(arts), 'ProtText':prot_text,
                                'Articles':','.join(arts)})
    



@ruffus.follows(aggregate_results)
def top_function():
    pass




if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Mutation Finding Code')
    
    parser.add_argument('--get-mapping', dest = 'getmapping', action = 'store_true',
                        default = False)
    args = parser.parse_args()
    
    if args.getmapping:
        ruffus.pipeline_run([download_files])
    else:
        ruffus.pipeline_run([process_mut_file], multiprocess = 4)





