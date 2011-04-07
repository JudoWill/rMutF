from threading import Semaphore, Timer
import re
import os
import unicodedata


class TimedSemaphore():
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

def touch(fname, times = None):
    with file(fname, 'a'):
        os.utime(fname, times)



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

