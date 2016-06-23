import logging

from django.utils.module_loading import import_string

from robust.utils import unwrap_payload
from .exceptions import Retry
from .signals import task_started

logger = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, task):
        """
        :type task: robust.models.Task
        """
        self.task = task


undefined = object()


class SimpleRunner(Runner):
    def call(self, fn, kwargs):
        """
        call task function, override in subclasses for custom signals etc
        """
        fn(**kwargs)

    # noinspection PyBroadException
    def run(self):
        payload = undefined
        try:
            fn = import_string(self.task.name)
            payload = unwrap_payload(self.task.payload or {})

            logger.info('run task %s(**%r)', self.task.name, payload)
            task_started.send_robust(
                sender=None,
                name=self.task.name,
                payload=payload,
                tags=self.task.tags
            )
            self.call(fn, payload)

        except Retry as retry:
            captured = False
            if self.task.retries is not None:
                self.task.retries -= 1
                if self.task.retries < 0:
                    self.task.mark_failed()
                    logger.exception('task %s(**%r) failed ', self.task.name,
                                     payload if payload is not undefined else self.task.payload)
                    captured = True

            if not captured:
                self.task.mark_retry(eta=retry.eta, delay=retry.delay, trace=retry.trace)
                logger.info('retry task %s(**%r) eta=%s delay=%s',
                            self.task.name, payload if payload is not undefined else self.task.payload,
                            retry.eta, retry.delay)

        except Exception:
            self.task.mark_failed()
            logger.exception('task %s(**%r) failed ', self.task.name,
                             payload if payload is not undefined else self.task.payload)

        else:
            self.task.mark_succeed()
            logger.info('task %s(**%r) succeed', self.task.name,
                        payload if payload is not undefined else self.task.payload)
