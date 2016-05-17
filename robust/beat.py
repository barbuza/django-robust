import signal
import threading
import time

from django.conf import settings
from django.db import close_old_connections
from django.utils.module_loading import import_string
from schedule import Scheduler


def schedule_task(task, tags):
    from .models import Task
    Task.objects.create(name=task, payload={}, tags=tags)


def get_scheduler():
    """
    :rtype scheduler.Scheduler
    """
    from .utils import TaskWrapper
    schedule_list = getattr(settings, 'ROBUST_SCHEDULE', None)
    if not schedule_list:
        raise RuntimeError("can't run beat with empty schedule")

    scheduler = Scheduler()

    for interval, task in schedule_list:
        task_cls = import_string(task)
        if not isinstance(task_cls, type) or not issubclass(task_cls, TaskWrapper):
            raise RuntimeError('{} is not decorated with @task'.format(task))

        # noinspection PyUnresolvedReferences
        scheduler.every(int(interval.total_seconds())) \
            .seconds.do(schedule_task, task, task_cls.tags)

    return scheduler


class BeatThread(threading.Thread):
    def __init__(self, scheduler):
        """
        :type scheduler: schedule.Scheduler
        """
        super(BeatThread, self).__init__(name='Beat')
        self.terminate = False
        self.scheduler = scheduler

    def run(self):
        try:
            while True:
                self.scheduler.run_pending()
                time.sleep(1)
                if self.terminate:
                    break
        finally:
            # noinspection PyProtectedMember
            if not isinstance(threading.current_thread(), threading._MainThread):
                close_old_connections()


def run_beat():
    scheduler = get_scheduler()
    thread = BeatThread(scheduler)

    def signal_handler(signum, frame):
        thread.terminate = True

    signal.signal(signal.SIGINT, signal_handler)

    thread.run()
