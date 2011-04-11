from suds.client import Client
import re



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

    regexp = re.compile('ids=&quot;([\w\d,]*)&quot;&gt')
    resdict = {}
    for sent in search_sent_list:
        if sent in resdict:
            yield resdict[sent]
        resp = client.service.contact(pipelineName = pipeline, 
                                        text = sent, 
                                        convertToHtml = False)
        res_list = regexp.findall(resp)
        res = []
        for r in res_list:
            res += r.split(',')
        resdict[sent] = res
        yield res
    


