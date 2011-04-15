from suds.client import Client
from BeautifulSoup import BeautifulStoneSoup
import re



def de_safe_xml(kinda_xml):
    """Converts an escaped HTML/XML into a more normal string."""

    htmlCodes = (
        ('&', '&amp;'),
        ('<', '&lt;'),
        ('>', '&gt;'),
        ('"', '&quot;'),
        ("'", '&#39;'))

    for rep, orig in htmlCodes:
        kinda_xml = kinda_xml.replace(orig, rep)
    return kinda_xml



def generate_whatizit_client():
    """Generates a SUDS client for the Whatizit webservice."""

    url = 'http://www.ebi.ac.uk/webservices/whatizit/ws?wsdl'
    client = Client(url, faults = False, retxml = True)
    return client
    


def ask_whatizit(search_sent_list, client = None, pipeline = 'whatizitSwissprot'):
    """A function which queries the Whatizit tool use the SOAP client.

    Care is taken to ensure that identical sentences are not querried 
    multiple times.

    Arguments:
    search_sent_list -- A LIST of sentences to search.
    client = None -- A SOAP client ... If None then one is created on the fly.
    pipeline = 'whatizitSwissprot' -- The pipeline to search.
    """

    if client is None:
        client = generate_whatizit_client()

    resdict = {}
    for sent in search_sent_list:
        if sent in resdict:
            yield resdict[sent]
        resp = client.service.contact(pipelineName = pipeline, 
                                        text = sent, 
                                        convertToHtml = False)
        soup = BeautifulStoneSoup(de_safe_xml(resp))
        groups = soup.findAll(soup.findAll('z:uniprot'))
        if groups:
            res = [(p.contents[0], p['ids'].split(',')) for p in groups]
        else:
            res = None

        resdict[sent] = res
        yield res
        
















