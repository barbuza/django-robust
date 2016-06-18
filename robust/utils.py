import json
from datetime import datetime, timedelta

from django.conf import settings
from django.db.models import Q
from django.utils.module_loading import import_string

from .exceptions import Retry as BaseRetry


def get_kwargs_processor_cls():
    processor_cls_path = getattr(settings, 'ROBUST_PAYLOAD_PROCESSOR', 'robust.utils.PayloadProcessor')
    return import_string(processor_cls_path)


def wrap_payload(payload):
    return get_kwargs_processor_cls().wrap_payload(payload)


def unwrap_payload(payload):
    return get_kwargs_processor_cls().unwrap_payload(payload)


class PayloadProcessor(object):
    @staticmethod
    def wrap_payload(payload):
        return payload

    @staticmethod
    def unwrap_payload(payload):
        return payload


class TaskWrapper(object):
    bind = False
    fn = None
    retries = None
    tags = []
    Retry = BaseRetry

    def __new__(cls, **kwargs):
        if cls.bind:
            return cls.fn(cls, **kwargs)
        return cls.fn(**kwargs)

    @classmethod
    def delay(cls, **kwargs):
        """
        :rtype robust.models.Task
        """
        wrapped_kwargs = wrap_payload(kwargs)

        if getattr(settings, 'ROBUST_ALWAYS_EAGER', False):
            json.dumps(wrapped_kwargs)  # checks kwargs is JSON serializable
            kwargs = unwrap_payload(wrapped_kwargs)
            if cls.bind:
                return cls.fn(cls, **kwargs)
            return cls.fn(**kwargs)

        from .models import Task
        return Task.objects.create(name='{}.{}'.format(cls.__module__, cls.__name__),
                                   payload=wrapped_kwargs, tags=cls.tags, retries=cls.retries)

    @classmethod
    def retry(cls, eta=None, delay=None):
        """
        :type eta: datetime.datetime
        :type delay: datetime.timedelta
        """
        raise cls.Retry(eta=eta, delay=delay)


def task(bind=False, tags=None, retries=None):
    def decorator(fn):
        retry_cls = type('{}{}'.format(fn.__name__, 'Retry'), (BaseRetry,), {})
        retry_cls.__module__ = fn.__module__

        task_cls = type(fn.__name__, (TaskWrapper,), {
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
def cleanup():
    from .models import Task
    now = datetime.now()
    succeed_task_expire = now - getattr(settings, 'ROBUST_SUCCEED_TASK_EXPIRE', timedelta(hours=1))
    failed_task_expire = now - getattr(settings, 'ROBUST_FAILED_TASK_EXPIRE', timedelta(weeks=1))

    Task.objects.filter(
        Q(status=Task.SUCCEED, updated_at__lte=succeed_task_expire) |
        Q(status=Task.FAILED, updated_at__lte=failed_task_expire)
    ).delete()
