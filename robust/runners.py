import logging

from django.utils.module_loading import import_string

from .exceptions import Retry
from .signals import task_started

logger = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, task):
        """
        :type task: robust.models.Task
        """
        self.task = task


class SimpleRunner(Runner):
    def call(self, fn, kwargs):
        """
        call task function, override in subclasses for custom signals etc
        """
        fn(**kwargs)

    # noinspection PyBroadException
    def run(self):
        try:
            fn = import_string(self.task.name)
            logger.info('run task %s(**%r)', self.task.name, self.task.payload)
            kwargs = self.task.payload or {}
            task_started.send_robust(
                sender=None,
                name=self.task.name,
                payload=self.task.payload,
                tags=self.task.tags
            )
            self.call(fn, kwargs)

        except Retry as retry:
            self.task.mark_retry(eta=retry.eta, delay=retry.delay)
            logger.info('retry task %s(**%r) eta=%s delay=%s',
                        self.task.name, self.task.payload, retry.eta, retry.delay)

        except Exception:
            self.task.mark_failed()
            logger.exception('task %s(**%r) failed ', self.task.name, self.task.payload)

        else:
            self.task.mark_succeed()
            logger.info('task %s(**%r) succeed', self.task.name, self.task.payload)
