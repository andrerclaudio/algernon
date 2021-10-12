# Build-in modules
import json
import logging
import os
import sys
import time
from datetime import timedelta
from threading import ThreadError, Thread

import requests
# Added modules
from pytictoc import TicToc

# Project modules
from Book.client import GoodReadsClient

# Print in software terminal
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(process)d | %(name)s | %(levelname)s:  %(message)s',
                    datefmt='%d/%b/%Y - %H:%M:%S')

logger = logging.getLogger(__name__)

MAX_NUMBER_OF_THREADS = 4


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

    # Store current working directory
    path = os.path.abspath('database')
    # Append current directory to the python path
    sys.path.append(path)

    gid = 1
    fail = []
    running_processes = []
    all_books = False

    while not all_books:

        running_processes.clear()
        new_threads = 0

        while new_threads < MAX_NUMBER_OF_THREADS:

            try:
                t = Thread(target=fetch_book, args=[good_reads, path, gid, fail])

                t.daemon = True  # Daemonize thread
                t.start()  # Start the execution
                running_processes.append(t)
                new_threads += 1

            except ThreadError as e:
                logger.exception('{}'.format(e), exc_info=False)

            finally:
                gid += 1
                if gid == 9:
                    gid -= 1
                    all_books = True
                    break

        # Wail for all processes to finish
        [t.join() for t in running_processes]
        logger.info('Fetched [{}] books!'.format(gid))

    with open(path + '/fail.json', 'w') as f:
        fail_books = {'fail': fail}
        json.dump(fail_books, f)


def fetch_book(good_reads, path, gid, fail):
    try:

        directory = str(gid)
        path = os.path.join(path, directory)

        # Create the directory
        os.mkdir(path)

        # Fetch book information
        book = good_reads.book(book_id=gid)

        url = book.image_url
        response = requests.get(url)
        if response.status_code == 200:
            with open(path + '/cover.jpg', 'wb') as f:
                f.write(response.content)

        with open(path + '/info.json', 'w') as f:

            info = {'authors': str(book.authors),
                    'average_rating': book.average_rating,
                    'description': book.description,
                    'edition_information': book.edition_information,
                    'format': book.format,
                    'gid': book.gid,
                    'image_url': book.image_url,
                    'is_ebook': book.is_ebook,
                    'isbn': book.isbn,
                    'isbn13': book.isbn13,
                    'language_code': book.language_code,
                    'link': book.link,
                    'num_pages': book.num_pages,
                    'popular_shelves': str(book.popular_shelves),
                    'publication_date': book.publication_date,
                    'publisher': book.publisher,
                    'rating_dist': book.rating_dist,
                    'ratings_count': book.ratings_count,
                    'reviews_widget': book.reviews_widget,
                    'series_works': book.series_works,
                    'similar_books': str(book.similar_books),
                    'small_image_url': book.small_image_url,
                    'text_reviews_count': book.text_reviews_count,
                    'title': book.title,
                    'work': book.work}

            json.dump(info, f)

    except Exception as e:
        logger.exception('{}'.format(e), exc_info=False)
        fail.append(gid)
