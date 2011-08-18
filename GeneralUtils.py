from threading import Semaphore, Timer
from subprocess import check_call
import re
import os
import unicodedata
import shlex

class TimedSemaphore():
    """A simple Timed Semaphore which only allows a specific number of requests per second."""
    def __init__(self, timer, total):
        self.sem = Semaphore(total)
        self.time = timer

    def release(self):
        self.sem.release()

    def acquire(self):
        self.sem.acquire()

    def __enter__(self):
        self.acquire()

    def __exit__(self, typ, value, traceback):
        Timer(self.time, self.release).start()

def get_known_mappings(ifile):
    
    mappingdict = {}
    with open(ifile) as handle:
        for line in handle:
            if not line.startswith('#'):
                parts = line.strip().split('|')
                for extra in parts[1:]:
                    mappingdict[extra] = parts[0]
    return mappingdict

def touch(fname, times = None):
    try:
        with file(fname, 'a'):
            os.utime(fname, times)
    except IOError: 
        check_call(shlex.split('touch %s' % fname))

def download_file(path, url, sort = False):
    """Uses wget to download a file from a url and unzip it."""
    
    fname = url.split('/')[-1].replace('.gz', '')
    with pushd(path):
        cmd = shlex.split('wget -N %s' % url)
        check_call(cmd)
        if path.endswith('.gz'):
            cmd = shlex.split('gzip -df %s' % fname+'.gz')
            check_call(cmd)
        if sort:
            ohandle = open(fname + '.sort', 'w')
            ihandle = open(fname)
            check_call(['sort'], stdin = ihandle, stdout = ohandle)


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters, 
    and converts spaces to hyphens.
    
    From Django's "django/template/defaultfilters.py".
    """

    _slugify_strip_re = re.compile(r'[^\w\s-]')
    _slugify_hyphenate_re = re.compile(r'[-\s]+')

    if not isinstance(value, unicode):
        value = unicode(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(_slugify_strip_re.sub('', value).strip().lower())
    return _slugify_hyphenate_re.sub('-', value)

class pushd():
    """Chnages a directory and then changes back when leaving the context."""

    def __init__(self, newpath):
        self.prev_path = os.getcwd()
        self.new_path = newpath        

    def __enter__(self):
        os.chdir(self.new_path)
    def __exit__(self, typ, value, tb):
        os.chdir(self.prev_path)        

