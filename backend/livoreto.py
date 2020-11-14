# Build-in modules
import configparser
import logging
import os
import time
from _signal import SIGINT, SIGTERM
from multiprocessing import Process, Manager, cpu_count as cpu
from multiprocessing import ProcessError
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


class InitQueue(object):
    """Incoming messages and pictures Queue initializer"""

    def __init__(self):
        """
        Initial state
        """
        self.message_queue = Queue()
        self.picture_queue = Queue()


class IdProcessQueue(object):
    """Hold the ChatId that are being precessed"""

    def __init__(self):
        self.set_id_pid = {}

    def exec_id(self, chat_id):
        """Return if a ChatId process is alive, if not, remove the CharId from the Dict"""
        if chat_id in self.set_id_pid.keys():
            p = self.set_id_pid[chat_id]
            if p.is_alive():
                return True
            else:
                del self.set_id_pid[chat_id]

        return False

    def set_pid(self, chat_id, process):
        """Hold a ChatId and its Process number"""
        self.set_id_pid[chat_id] = process


def error(update, context):
    """Log Errors caused by Updates"""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


class InitializeTelegram(object):
    """Telegram Bot initializer"""

    def __init__(self, sys_queue):
        # Configuring bot
        if WORK_MODE == 'dev&cloud':
            telegram_token = os.environ['DEV']
        elif WORK_MODE == 'prod&cloud':
            telegram_token = os.environ['DEFAULT']
        else:
            config = configparser.ConfigParser()
            config.read_file(open('config.ini'))
            if WORK_MODE == 'dev&rasp':
                telegram_token = config['DEV']['token']
            else:
                # 'prod&rasp'
                telegram_token = config['DEFAULT']['token']

        self.queue = sys_queue

        # Connecting to Telegram API
        self.updater = Updater(token=telegram_token, use_context=True)
        dispatcher = self.updater.dispatcher

        # Creating handlers
        pic_handler = MessageHandler(Filters.photo, lambda update, context: telegram_receive_pic(update, self.queue))
        msg_handler = MessageHandler(Filters.text, lambda update, context: telegram_receive_msg(update, self.queue))

        # Add handlers to Telegram Dispatcher
        dispatcher.add_handler(pic_handler)
        # Message handler must be the last one
        dispatcher.add_handler(msg_handler)

        # log all errors
        dispatcher.add_error_handler(error)

        if WORK_MODE == 'dev&cloud' or WORK_MODE == 'prod&cloud':
            port = int(os.environ.get('PORT', '8443'))
            self.updater.start_webhook(listen="0.0.0.0", port=port, url_path=telegram_token)
            self.updater.bot.setWebhook("https://livoreto.herokuapp.com/" + telegram_token)
        else:
            # and then, start pulling for new messages
            self.updater.start_polling(clean=True)

        while not self.updater.running:
            pass


def run(queue, telegram, pending_jobs):
    """Method that runs forever"""

    # Start ID queue in order to run just one task per Id
    id_queue = IdProcessQueue()
    # Start Long Process queue (book recommendation system)
    p_running = IdProcessQueue()

    while True:

        if not queue.message_queue.empty():

            update = queue.message_queue.get()
            chat_id = update.message.chat_id
            # Check if the ChatId is already with a task being processed
            if id_queue.exec_id(chat_id):
                queue.message_queue.put(update)
            else:
                try:
                    p = Process(target=message_digest,
                                args=(update, pending_jobs),
                                name='Message digest')
                    p.daemon = True
                    p.start()
                    id_queue.set_pid(chat_id, p)
                except ProcessError as e:
                    logger.exception('{}'.format(e), exc_info=False)
                finally:
                    # Show the message queue size
                    logger.info('[Message queue: {}]'.format(queue.message_queue.qsize()))
        # --------------------------------------------------------------------------------------------------
        if not queue.picture_queue.empty():

            update = queue.picture_queue.get()
            chat_id = update.message.chat_id
            # Check if the ChatId is already with a task being processed
            if id_queue.exec_id(chat_id):
                queue.picture_queue.put(update)
            else:
                try:
                    p = Process(target=picture_digest,
                                args=(update, pending_jobs),
                                name='Picture digest')
                    p.daemon = True
                    p.start()
                    id_queue.set_pid(chat_id, p)
                except ProcessError as e:
                    logger.exception('{}'.format(e), exc_info=False)
                finally:
                    # Show the picture queue size
                    logger.info('[Picture queue: {}]'.format(queue.picture_queue.qsize()))
        # Check if there is a pending job ------------------------------------------------------------------
        if len(pending_jobs) > 0:
            logger.info('[Pending job queue: {}]'.format(len(pending_jobs)))
            job, chat_id = pending_jobs.popitem()
            # Check if the ChatId is already with a task being processed (
            if p_running.exec_id(chat_id):
                logger.info('Task discarded due to job duplicity for the same Chat Id!')
            else:
                try:
                    p = Process(target=jobs_digest,
                                args=(chat_id, telegram, job),
                                name='Job digest')
                    p.daemon = True
                    p.start()
                    p_running.set_pid(chat_id, p)
                except ProcessError as e:
                    logger.exception('{}'.format(e))
            # Show the background jobs queue size
            logger.info('[Pending job queue: {}]'.format(len(pending_jobs)))
        # --------------------------------------------------------------------------------------------------


class ThreadingProcessQueue(object):
    """The run() method will be started and it will run in the background until the application exits"""

    def __init__(self, sys_queue, telegram, pending_jobs):
        """Constructor"""
        self.queue = sys_queue
        self.telegram = telegram
        self.pending_jobs = pending_jobs

        try:
            thread = Thread(target=run,
                            args=[self.queue, self.telegram, self.pending_jobs],
                            name='Processor')

            thread.daemon = True  # Daemonize thread
            thread.start()  # Start the execution
        except ThreadError as e:
            logger.exception('{}'.format(e))


def application():
    """System initialization"""
    # All application has its initialization from here
    logger.info('Main application is running!')

    if WORK_MODE == 'prod&rasp':
        # When running in a raspberry environment, create a delay to ensure all others systems are running
        logger.info('Counting ...')
        time.sleep(5)
        logger.info('Done!')

    # Count available CPUs
    logger.info("Number of cpu: %s", cpu())

    # Initialize Queues
    # sys_queue = InitQueue()

    # Initialize pending job dictionary
    # pending_jobs = Manager().dict()

    # Initializing Telegram
    # _telegram = InitializeTelegram(sys_queue)

    # Start processing all information
    # ThreadingProcessQueue(sys_queue, _telegram, pending_jobs)
    ThreadingRecommendation()

    # Get ready for any further stop signals
    # _telegram.updater.idle(stop_signals=(SIGINT, SIGTERM))


def telegram_receive_msg(update, sys_queue):
    """All received Telegram messages is queued here"""
    try:
        logger.debug('{}'.format(update.message.chat_id))

        # Message Queue
        sys_queue.message_queue.put(update)
        logger.info('[Message queue: {}]'.format(int(sys_queue.message_queue.qsize())))
    except Exception as e:
        logger.exception('{}'.format(e))


def telegram_receive_pic(update, sys_queue):
    """All received Telegram pictures is queued here"""
    try:
        logger.debug('{}'.format(update.message.chat_id))

        # Picture Queue
        sys_queue.picture_queue.put(update)
        logger.info('[Picture queue: {}]'.format(sys_queue.picture_queue.qsize()))
    except Exception as e:
        logger.exception('{}'.format(e))


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