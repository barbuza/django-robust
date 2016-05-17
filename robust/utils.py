from .exceptions import Retry as BaseRetry


class TaskWrapper(object):
    bind = False
    fn = None
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
        from .models import Task
        return Task.objects.create(name='{}.{}'.format(cls.__module__, cls.__name__),
                                   payload=kwargs, tags=cls.tags)

    @classmethod
    def retry(cls, eta=None, delay=None):
        """
        :type eta: datetime.datetime
        :type delay: datetime.timedelta
        """
        raise cls.Retry(eta=eta, delay=delay)


def task(bind=False, tags=None):
    def decorator(fn):
        retry_cls = type('{}{}'.format(fn.__name__, 'Retry'), (BaseRetry,), {})
        retry_cls.__module__ = fn.__module__

        task_cls = type(fn.__name__, (TaskWrapper,), {
            'fn': staticmethod(fn),
            'tags': tags,
            'bind': bind,
            'Retry': retry_cls
        })
        task_cls.__module__ = fn.__module__

        return task_cls

    return decorator
