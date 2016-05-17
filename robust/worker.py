import logging
import select
import signal
import threading
import time

from django.conf import settings
from django.db import transaction, connection, close_old_connections

from .beat import BeatThread, get_scheduler
from .models import Task

logger = logging.getLogger(__name__)


class Stop(Exception):
    pass


class WorkerLimit(object):
    def __init__(self, limit):
        """
        :type limit: int
        """
        self.lock = threading.Lock()
        self.limit = limit

    def dec(self, amount):
        with self.lock:
            self.limit -= amount

    def should_terminate(self):
        with self.lock:
            return self.limit <= 0


class WorkerThread(threading.Thread):
    def __init__(self, number, runner_cls, bulk, worker_limit):
        """
        :type number: int
        :type runner_cls: type
        :type bulk: int
        :type worker_limit: WorkerLimit
        """
        super(WorkerThread, self).__init__(name='WorkerThread-{}'.format(number))
        self.runner_cls = runner_cls
        self.bulk = bulk
        self.worker_limit = worker_limit
        self.terminate = False

    def should_terminate(self):
        if self.terminate:
            return True
        if self.worker_limit:
            if self.worker_limit.should_terminate():
                return True
        return False

    # noinspection PyBroadException
    def run(self):
        try:
            notify_timeout = getattr(settings, 'ROBUST_NOTIFY_TIMEOUT', 10)
            worker_failure_timeout = getattr(settings, 'ROBUST_WORKER_FAILURE_TIMEOUT', 5)

            while True:
                try:
                    if self.should_terminate():
                        raise Stop()

                    with transaction.atomic():
                        tasks = Task.objects.next(limit=self.bulk)
                        logger.debug('%s got tasks %r', self.name, tasks)
                        if self.worker_limit:
                            self.worker_limit.dec(amount=len(tasks))

                        for task in tasks:
                            runner = self.runner_cls(task)
                            runner.run()

                    if self.should_terminate():
                        raise Stop()

                    if not tasks:
                        with connection.cursor() as cursor:
                            cursor.execute('LISTEN robust')

                        logger.debug('listen for postgres events')
                        select.select([connection.connection], [], [], notify_timeout)

                except Stop:
                    break

                except Exception:
                    logger.error('%s exception ', self.name, exc_info=True)
                    time.sleep(worker_failure_timeout)

            logger.debug('terminating %s', self.name)
        finally:
            close_old_connections()


def run_worker(concurrency, bulk, limit, runner_cls, beat):
    """
    :type concurrency: int
    :type bulk: int
    :type limit: int
    :type runner_cls: type
    """

    worker_limit = None
    if limit:
        worker_limit = WorkerLimit(limit)

    threads = []

    if beat:
        scheduler = get_scheduler()
        thread = BeatThread(scheduler)
        threads.append(thread)
        thread.start()

    for number in range(concurrency):
        thread = WorkerThread(number, runner_cls, bulk, worker_limit)
        threads.append(thread)
        thread.start()

    def terminate():
        for t in threads:
            t.terminate = True
        with connection.cursor() as cursor:
            cursor.execute('NOTIFY robust')

    def signal_handler(signum, frame):
        logger.warning('terminate worker')
        terminate()

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        for thread in threads:
            if thread.is_alive():
                break
        else:
            break
        time.sleep(1)
        if worker_limit:
            if worker_limit.should_terminate():
                terminate()
