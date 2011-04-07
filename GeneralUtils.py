from threading import Semaphore, Timer



class TimedSemaphore():
    def __init__(self, timer, total):
        self.sem = Semaphore(total)
        self.time = timer

    def release(self):
        print 'releasing'
        self.sem.release()

    def acquire(self):
        self.sem.acquire()

    def __enter__(self):
        print 'waiting'
        self.acquire()

    def __exit__(self, typ, value, traceback):
        Timer(self.time, self.release).start()
