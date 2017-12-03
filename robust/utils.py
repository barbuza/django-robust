import json
import sys
import traceback
from datetime import datetime, timedelta
from typing import Any, Callable, ClassVar, List, Optional, Type
import inspect

from django.conf import settings
from django.utils import timezone
# from django.utils.inspect import getargspec
from django.utils.module_loading import import_string

import robust
from .exceptions import Retry as BaseRetry


def get_kwargs_processor_cls() -> Type['PayloadProcessor']:
    processor_cls_path = getattr(settings, 'ROBUST_PAYLOAD_PROCESSOR',
                                 'robust.utils.PayloadProcessor')
    return import_string(processor_cls_path)


def wrap_payload(payload: dict) -> Any:
    return get_kwargs_processor_cls().wrap_payload(payload)


def unwrap_payload(payload: Any) -> dict:
    return get_kwargs_processor_cls().unwrap_payload(payload)


class PayloadProcessor:
    @staticmethod
    def wrap_payload(payload: dict) -> Any:
        return payload

    @staticmethod
    def unwrap_payload(payload: Any) -> dict:
        return payload


class ArgsWrapper:
    def __init__(self, wrapper: Type['TaskWrapper'],
                 eta: Optional[datetime] = None,
                 delay: Optional[timedelta] = None) -> None:
        self.wrapper = wrapper

        if eta and delay:
            raise RuntimeError('both eta and delay provided')

        if delay:
            eta = timezone.now() + delay

        self.eta = eta

    def delay(self, *args: Any, **kwargs: Any) -> 'robust.models.Task':
        return self.wrapper.delay_with_task_kwargs({'eta': self.eta},
                                                   *args, **kwargs)


class TaskWrapper:
    bind: ClassVar[bool]
    fn: ClassVar[Callable[..., Any]]
    retries: ClassVar[Optional[int]]
    tags: ClassVar[List[str]] = []
    Retry: ClassVar[Type[BaseRetry]]

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls.bind:
            return cls.fn(cls, *args, **kwargs)
        return cls.fn(*args, **kwargs)

    @classmethod
    def with_task_kwargs(cls, eta: Optional[datetime] = None,
                         delay: Optional[timedelta] = None) -> ArgsWrapper:
        return ArgsWrapper(cls, eta=eta, delay=delay)

    @classmethod
    def delay_with_task_kwargs(cls, _task_kwargs: dict, *args: Any,
                               **kwargs: Any) -> 'robust.models.Task':
        name = '{}.{}'.format(cls.__module__, cls.__name__)

        if args:
            fn_args: List[inspect.Parameter] = list(inspect.signature(cls.fn)
                                                    .parameters.values())
            if cls.bind:
                fn_args = fn_args[1:]

            if len(args) > len(fn_args):
                raise TypeError(f'wrong args number passed for {name}')

            positional = fn_args[:len(args)]
            for arg in positional:
                if arg.name in kwargs:
                    raise TypeError(f'{arg.name} used as positional '
                                    f'argument for {name}')

            kwargs = dict(kwargs)
            for arg, value in zip(fn_args, args):
                kwargs[arg.name] = value

        wrapped_kwargs = wrap_payload(kwargs)

        if getattr(settings, 'ROBUST_ALWAYS_EAGER', False):
            json.dumps(wrapped_kwargs)  # checks kwargs is JSON serializable
            kwargs = unwrap_payload(wrapped_kwargs)
            if cls.bind:
                return cls.fn(cls, **kwargs)
            return cls.fn(**kwargs)

        from .models import Task
        _kwargs = {
            'tags': cls.tags,
            'retries': cls.retries
        }
        _kwargs.update(_task_kwargs)
        return Task.objects.create(name=name, payload=wrapped_kwargs,
                                   **_kwargs)

    @classmethod
    def delay(cls, *args: Any, **kwargs: Any) -> 'robust.models.Task':
        return cls.delay_with_task_kwargs({}, *args, **kwargs)

    @classmethod
    def retry(cls, eta: Optional[datetime] = None,
              delay: Optional[timedelta] = None) -> None:
        etype, value, tb = sys.exc_info()
        trace = None
        if etype and value and tb:
            trace = ''.join(traceback.format_exception(etype, value, tb))
        try:
            raise cls.Retry(eta=eta, delay=delay, trace=trace)
        finally:
            del tb


def task(bind: bool = False, tags: Optional[List[str]] = None,
         retries: Optional[int] = None) \
        -> Callable[['function'], Type[TaskWrapper]]:
    def decorator(fn: 'function') -> Type[TaskWrapper]:
        retry_cls = type('{}{}'.format(fn.__name__, 'Retry'), (BaseRetry,), {})
        retry_cls.__module__ = fn.__module__

        task_cls: Type[TaskWrapper] = type(fn.__name__, (TaskWrapper,), {
            'fn': staticmethod(fn),
            'retries': retries,
            'tags': tags,
            'bind': bind,
            'Retry': retry_cls
        })
        task_cls.__module__ = fn.__module__

        return task_cls

    return decorator


@task()
def cleanup() -> None:
    from .models import Task
    now = timezone.now()
    succeed_task_expire = now - getattr(settings, 'ROBUST_SUCCEED_TASK_EXPIRE',
                                        timedelta(hours=1))
    troubled_task_expire = now - getattr(settings, 'ROBUST_FAILED_TASK_EXPIRE',
                                         timedelta(weeks=1))

    Task.objects \
        .filter(events__status__in={Task.FAILED, Task.RETRY},
                status__in={Task.FAILED, Task.SUCCEED},
                updated_at__lte=troubled_task_expire) \
        .delete()

    Task.objects \
        .exclude(events__status__in={Task.FAILED, Task.RETRY}) \
        .filter(status=Task.SUCCEED, updated_at__lte=succeed_task_expire) \
        .delete()
