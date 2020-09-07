import signal
import threading
import time
from datetime import timedelta
from typing import Any, List, Tuple, Type, cast

from django.conf import settings
from django.db import close_old_connections
from django.utils.module_loading import import_string
from schedule import Scheduler


def schedule_task(task: str, tags: List[str]) -> None:
    from .models import Task

    Task.objects.create(name=task, payload={}, tags=tags)


def get_scheduler() -> Scheduler:
    from .models import TaskWrapper

    schedule_list: List[Tuple[timedelta, str]] = getattr(
        settings, "ROBUST_SCHEDULE", None
    )
    if not schedule_list:
        raise RuntimeError("can't run beat with empty schedule")

    scheduler = Scheduler()

    for interval, task in schedule_list:
        task_cls: Type[TaskWrapper] = import_string(task)
        if not isinstance(task_cls, type) or not issubclass(task_cls, TaskWrapper):
            raise RuntimeError("{} is not decorated with @task".format(task))

        if isinstance(interval, timedelta):
            # noinspection PyUnresolvedReferences
            scheduler.every(int(interval.total_seconds())).seconds.do(
                schedule_task, task, task_cls.tags
            )
        else:
            interval(scheduler).do(schedule_task, task, task_cls.tags)

    return scheduler


class BeatThread(threading.Thread):
    def __init__(self, scheduler: Scheduler) -> None:
        super(BeatThread, self).__init__(name="Beat")
        self.terminate = False
        self.scheduler = scheduler

    def run(self) -> None:
        try:
            while True:
                self.scheduler.run_pending()
                time.sleep(1)
                if self.terminate:
                    break
        finally:
            # noinspection PyProtectedMember
            if not isinstance(
                threading.current_thread(), cast(Any, threading)._MainThread
            ):
                close_old_connections()


def run_beat() -> None:
    scheduler = get_scheduler()
    thread = BeatThread(scheduler)

    def signal_handler(*_args: Any) -> None:
        thread.terminate = True

    signal.signal(signal.SIGINT, signal_handler)

    thread.run()
