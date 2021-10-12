# Build-in modules
import logging
import time
from datetime import timedelta
from threading import ThreadError, Thread

# Added modules
from pytictoc import TicToc

# Project modules
from Book.client import GoodReadsClient

# Print in software terminal
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(process)d | %(name)s | %(levelname)s:  %(message)s',
                    datefmt='%d/%b/%Y - %H:%M:%S')

logger = logging.getLogger(__name__)


class ElapsedTime(object):
    """
    Measure the elapsed time between Tic and Toc
    """
    def __init__(self):
        self.t = TicToc()
        self.t.tic()

    def elapsed(self):
        _elapsed = self.t.tocvalue()
        d = timedelta(seconds=_elapsed)
        logger.info('< {} >'.format(d))


class ThreadingProcessQueue(object):
    """
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self, interval):
        """
        Constructor
        """
        self.interval = interval

        t = Thread(target=run, args=(self.interval,), name='Thread_name')
        t.daemon = True  # Daemonize thread
        t.start()  # Start the execution


def run(interval):
    """ Method that runs forever """
    while True:
        try:
            time.sleep(interval)

        except ThreadError as e:
            logger.exception('{}'.format(e))

        finally:
            pass


def application():
    """" All application has its initialization from here """
    logger.info('Main application is running!')

    good_reads = GoodReadsClient()

    gid = 0

    # ISBN related functions
    book = isbn_lookup(gid, good_reads)
    # Check for a valid information
    if len(book) > 0:
        pass


def isbn_lookup(gid, good_reads):
    """
    Fetch in Good Reads for a given ISBN code
    """
    book = {}

    try:
        book = good_reads.book(book_id=gid)

    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)

    finally:
        return book
