import logging
from typing import Any, Callable

from django.utils.module_loading import import_string

from . import models, signals
from .exceptions import Retry
from .models import unwrap_payload

logger = logging.getLogger(__name__)


class Runner:
    def __init__(self, task: models.Task) -> None:
        self.task = task

    def run(self) -> None:
        raise NotImplementedError()


undefined = object()


class SimpleRunner(Runner):
    def call(self, fn: Callable[..., Any], kwargs: dict) -> None:
        """
        call task function, override in subclasses for custom signals etc
        """
        fn(**kwargs)

    # noinspection PyBroadException
    def run(self) -> None:
        payload = undefined
        log_payload = self.task.payload

        def send_signal(signal: signals.Signal) -> None:
            signal.send_robust(
                sender=None,
                pk=self.task.pk,
                name=self.task.name,
                payload=payload if payload is not undefined else None,
                raw_payload=self.task.payload,
                tags=self.task.tags,
            )

        try:
            fn = import_string(self.task.name)
            payload = log_payload = unwrap_payload(self.task.payload or {})

            logger.info("run task %s(**%r)", self.task.name, payload)
            send_signal(signals.task_started)
            self.call(fn, payload)

        except Retry as retry:
            captured = False
            if self.task.retries is not None:
                self.task.retries -= 1
                if self.task.retries < 0:
                    self.task.mark_failed()
                    logger.exception(
                        "task %s(**%r) failed ", self.task.name, log_payload
                    )
                    send_signal(signals.task_failed)
                    captured = True

            if not captured:
                self.task.mark_retry(
                    eta=retry.eta, delay=retry.delay, trace=retry.trace
                )
                logger.info(
                    "retry task %s(**%r) eta=%s delay=%s",
                    self.task.name,
                    log_payload,
                    retry.eta,
                    retry.delay,
                )
                send_signal(signals.task_retry)

        except Exception:
            self.task.mark_failed()
            logger.exception("task %s(**%r) failed ", self.task.name, log_payload)
            send_signal(signals.task_failed)

        else:
            self.task.mark_succeed()
            logger.info("task %s(**%r) succeed", self.task.name, log_payload)
            send_signal(signals.task_succeed)
