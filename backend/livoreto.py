# Build-in modules
import configparser
import logging
import os
import time
from _signal import SIGINT, SIGTERM
from multiprocessing import Process, Manager, cpu_count as cpu, ProcessError
from queue import Queue
from threading import Thread, ThreadError

# Added modules
from telegram.ext import Updater, MessageHandler, Filters

# Project modules
from Book.client import WORK_MODE
from Machine_learning.recommender_system import recommendation_tree
from system_digest import message_processor_machine as message_digest, picture_processor_machine as picture_digest, \
    pending_jobs_processor_machine as jobs_digest

if WORK_MODE == 'prod&rasp' or WORK_MODE == 'prod&cloud':
    # Print in file
    logging.basicConfig(filename='logs.log',
                        filemode='w',
                        level=logging.INFO,
                        format='%(asctime)s | %(process)d | %(name)s | %(levelname)s:  %(message)s',
                        datefmt='%d/%b/%Y - %H:%M:%S')
else:
    # Print in software terminal
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(process)d | %(name)s | %(levelname)s:  %(message)s',
                        datefmt='%d/%b/%Y - %H:%M:%S')

logger = logging.getLogger(__name__)


def application():
    """System initialization"""

    # All application has its initialization from here
    logger.info('Main application is running!')
    # Count available CPUs
    logger.info("Number of cpu: %s", cpu())
    # Start a new Thread with the desired Task.
    ThreadingRecommendation()


class ThreadingRecommendation(object):
    """The run() method will be started and it will run in the background until the application exits"""

    def __init__(self):
        """Constructor"""

        try:
            thread = Thread(target=run_recommendation,
                            args=[],
                            name='Processor')

            thread.daemon = True  # Daemonize thread
            thread.start()  # Start the execution
            thread.join()
        except ThreadError as e:
            logger.exception('{}'.format(e))


def run_recommendation():
    """Method that runs forever"""

    while True:
        try:
            p = Process(target=recommendation_tree,
                        args=(),
                        name='Recommendation process!')
            p.daemon = True
            p.start()
        except ProcessError as e:
            logger.exception('{}'.format(e), exc_info=False)