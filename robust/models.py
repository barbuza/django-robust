import inspect
import json
import struct
import sys
import traceback
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    cast,
)

from django.conf import settings
from django.contrib.postgres.fields import ArrayField, JSONField

# noinspection PyProtectedMember
from django.core.cache import caches
from django.db import connection, models
from django.utils import timezone
from django.utils.module_loading import import_string
from django_redis.cache import RedisCache
from redis import Redis

from .exceptions import Retry as BaseRetry
from .exceptions import TaskTransactionError


class TaskQuerySet(models.QuerySet):
    def with_fails(self) -> "TaskQuerySet":
        qs = self.filter(events__status__in=Task.TROUBLED_STATUSES).distinct()
        return cast(TaskQuerySet, qs)

    def without_fails(self) -> "TaskQuerySet":
        qs = self.exclude(events__status__in=Task.TROUBLED_STATUSES).distinct()
        return cast(TaskQuerySet, qs)

    def next(self, limit: int = 1, eta: Optional[datetime] = None) -> List["Task"]:
        if not connection.in_atomic_block:
            raise TaskTransactionError(
                "Task.objects.next() must be used inside transaction"
            )

        if eta is None:
            eta = timezone.now()

        status_cond = models.Q(status__in=[Task.PENDING, Task.RETRY])
        eta_cond = models.Q(eta=None) | models.Q(eta__lte=eta)

        qs = (
            self.select_for_update(skip_locked=True)
            .filter(status_cond & eta_cond)
            .order_by("pk")
        )

        tasks = list(qs[:limit])

        allowed_tasks: List[Task] = []

        for t in tasks:
            tags_eta = calc_tags_eta(t.tags)
            if tags_eta:
                max_eta = max(tags_eta.values())
                if max_eta >= cast(datetime, eta):
                    t.eta = max_eta
                    t.save(update_fields={"eta"})
                    continue
            allowed_tasks.append(t)

        return allowed_tasks


class Task(models.Model):
    PENDING = 0
    RETRY = 1
    SUCCEED = 2
    FAILED = 3

    TROUBLED_STATUSES = [RETRY, FAILED]

    STATUS_CHOICES = [
        (PENDING, "pending"),
        (RETRY, "retry"),
        (SUCCEED, "succeed"),
        (FAILED, "failed"),
    ]

    status = models.PositiveSmallIntegerField(
        choices=STATUS_CHOICES, db_index=True, default=PENDING
    )
    retries = models.IntegerField(null=True)
    name = models.TextField()
    tags = ArrayField(models.TextField())
    payload = JSONField()
    traceback = models.TextField(null=True)
    eta = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = TaskQuerySet.as_manager()

    def _format_traceback(self) -> Optional[str]:
        etype, value, tb = sys.exc_info()
        try:
            if etype and value and tb:
                return "".join(traceback.format_exception(etype, value, tb))
            return None
        finally:
            del tb

    def mark_retry(
        self,
        eta: Optional[datetime] = None,
        delay: Optional[timedelta] = None,
        trace: Optional[str] = None,
    ) -> None:
        """
        mark task for retry with given {eta} or {delay}
        """
        if delay is not None:
            eta = timezone.now() + delay
        self.eta = eta
        self.status = self.RETRY
        self.traceback = trace
        self.save(update_fields={"eta", "status", "traceback", "retries", "updated_at"})

    def mark_succeed(self) -> None:
        """
        mark task as succeed
        """
        self.status = self.SUCCEED
        self.traceback = None
        self.save(update_fields={"status", "traceback", "updated_at"})

    def mark_failed(self) -> None:
        """
        mark task as failed
        """
        self.status = self.FAILED
        self.traceback = self._format_traceback()
        self.save(update_fields={"status", "traceback", "updated_at"})

    @property
    def log(self) -> str:
        """
        task log
        """
        items = []
        for idx, event in enumerate(self.log_events):
            if idx == 0:
                action = "created"
            else:
                action = event.get_status_display()
            items.append(f"{event.created_at} {action}")
        return "\n".join(items)

    @property
    def log_events(self) -> Iterable["TaskEvent"]:
        return self.events.order_by("pk")

    def __repr__(self) -> str:
        chunks = [self.name, f"#{self.pk}", self.get_status_display()]
        if self.eta:
            chunks.insert(2, str(self.eta))
        return f'<Task {" ".join(chunks)}>'

    __str__ = __repr__

    class Meta:
        index_together = [("status", "eta")]


class TaskEvent(models.Model):
    task = models.ForeignKey(Task, related_name="events", on_delete=models.CASCADE)
    created_at = models.DateTimeField()
    status = models.PositiveSmallIntegerField(choices=Task.STATUS_CHOICES)
    eta = models.DateTimeField(blank=True, null=True)
    traceback = models.TextField(null=True)


def get_kwargs_processor_cls() -> Type["PayloadProcessor"]:
    processor_cls_path = getattr(
        settings, "ROBUST_PAYLOAD_PROCESSOR", "robust.models.PayloadProcessor"
    )
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
    def __init__(
        self,
        wrapper: Type["TaskWrapper"],
        eta: Optional[datetime] = None,
        delay: Optional[timedelta] = None,
    ) -> None:
        self.wrapper = wrapper

        if eta and delay:
            raise RuntimeError("both eta and delay provided")

        if delay:
            eta = timezone.now() + delay

        self.eta = eta

    def delay(self, *args: Any, **kwargs: Any) -> Task:
        return self.wrapper.delay_with_task_kwargs({"eta": self.eta}, *args, **kwargs)


TRANSACTION_SCHEDULE_KEY = "robust_scheduled_tasks"


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
    def with_task_kwargs(
        cls, eta: Optional[datetime] = None, delay: Optional[timedelta] = None
    ) -> ArgsWrapper:
        return ArgsWrapper(cls, eta=eta, delay=delay)

    @classmethod
    def delay_with_task_kwargs(
        cls, _task_kwargs: dict, *args: Any, **kwargs: Any
    ) -> Task:
        name = "{}.{}".format(cls.__module__, cls.__name__)

        if args:
            fn_args: List[inspect.Parameter] = list(
                inspect.signature(cls.fn).parameters.values()
            )
            if cls.bind:
                fn_args = fn_args[1:]

            if len(args) > len(fn_args):
                raise TypeError(f"wrong args number passed for {name}")

            positional = fn_args[: len(args)]
            for arg in positional:
                if arg.name in kwargs:
                    raise TypeError(
                        f"{arg.name} used as positional argument for {name}"
                    )

            kwargs = dict(kwargs)
            for arg, value in zip(fn_args, args):
                kwargs[arg.name] = value

        wrapped_kwargs = wrap_payload(kwargs)

        if getattr(settings, "ROBUST_ALWAYS_EAGER", False):
            json.dumps(wrapped_kwargs)  # checks kwargs is JSON serializable
            kwargs = unwrap_payload(wrapped_kwargs)
            if cls.bind:
                return cls.fn(cls, **kwargs)
            return cls.fn(**kwargs)

        _kwargs = {"tags": cls.tags, "retries": cls.retries}
        _kwargs.update(_task_kwargs)

        scheduled_tasks = None
        if connection.in_atomic_block:
            if not hasattr(connection, TRANSACTION_SCHEDULE_KEY):
                setattr(connection, TRANSACTION_SCHEDULE_KEY, {})

                @connection.on_commit
                def transaction_cleanup() -> None:
                    delattr(connection, TRANSACTION_SCHEDULE_KEY)

            schedule = getattr(connection, TRANSACTION_SCHEDULE_KEY)
            schedule.setdefault(name, [])
            scheduled_tasks = schedule[name]
            for scheduled, payload, kw in scheduled_tasks:
                if payload == wrapped_kwargs and kw == _kwargs:
                    return scheduled

        new_task = Task.objects.create(name=name, payload=wrapped_kwargs, **_kwargs)
        if scheduled_tasks is not None:
            scheduled_tasks.append((new_task, wrapped_kwargs, _kwargs))
        return new_task

    @classmethod
    def delay(cls, *args: Any, **kwargs: Any) -> Task:
        return cls.delay_with_task_kwargs({}, *args, **kwargs)

    @classmethod
    def retry(
        cls, eta: Optional[datetime] = None, delay: Optional[timedelta] = None
    ) -> None:
        etype, value, tb = sys.exc_info()
        trace = None
        if etype and value and tb:
            trace = "".join(traceback.format_exception(etype, value, tb))
        try:
            raise cls.Retry(eta=eta, delay=delay, trace=trace)
        finally:
            del tb


def task(
    bind: bool = False, tags: Optional[List[str]] = None, retries: Optional[int] = None
) -> Callable[[Callable[..., None]], Type[TaskWrapper]]:
    def decorator(fn: Callable[..., None]) -> Type[TaskWrapper]:
        retry_cls = type("{}{}".format(fn.__name__, "Retry"), (BaseRetry,), {})
        retry_cls.__module__ = fn.__module__

        task_cls: Type[TaskWrapper] = type(
            fn.__name__,
            (TaskWrapper,),
            {
                "fn": staticmethod(fn),
                "retries": retries,
                "tags": tags,
                "bind": bind,
                "Retry": retry_cls,
            },
        )
        task_cls.__module__ = fn.__module__

        return task_cls

    return decorator


def tag_cache_key(tag: str) -> str:
    return f"robust_tag_{tag}"


def calc_tags_eta(tags: List[str]) -> Dict[str, datetime]:
    if not tags:
        return {}

    rate_limit_config = getattr(settings, "ROBUST_RATE_LIMIT", None)
    if not rate_limit_config:
        return {}

    configured_tags: Dict[str, Tuple[int, timedelta]] = {}
    for tag in tags:
        if tag in rate_limit_config:
            configured_tags[tag] = rate_limit_config[tag]

    if not configured_tags:
        return {}

    cache: RedisCache = caches["robust"]
    client: Redis = cache.client.get_client()

    etas: Dict[str, datetime] = {}

    for tag, (count, interval) in configured_tags.items():
        first_run_ts_bytes = client.lindex(tag_cache_key(tag), count - 1)
        if first_run_ts_bytes is None:
            continue

        (first_run_ts,) = struct.unpack("I", first_run_ts_bytes)
        first_run = datetime.fromtimestamp(first_run_ts, tz=timezone.utc)
        next_run = first_run + interval
        etas[tag] = next_run

    return etas


def save_tag_run(tag: str, dt: datetime) -> None:
    assert dt.tzinfo is timezone.utc
    assert dt.microsecond == 0

    rate_limit_config = getattr(settings, "ROBUST_RATE_LIMIT", None)
    if not rate_limit_config:
        return None
    if tag not in rate_limit_config:
        return None

    cache: RedisCache = caches["robust"]
    client: Redis = cache.client.get_client()
    cache_key = tag_cache_key(tag)

    pipeline = client.pipeline(transaction=False)
    pipeline.lpush(cache_key, struct.pack("I", int(dt.timestamp())))
    pipeline.ltrim(cache_key, 0, rate_limit_config[tag][0] - 1)
    pipeline.execute()


@task()
def cleanup() -> None:
    now = timezone.now()
    succeed_task_expire = now - getattr(
        settings, "ROBUST_SUCCEED_TASK_EXPIRE", timedelta(hours=1)
    )
    troubled_task_expire = now - getattr(
        settings, "ROBUST_FAILED_TASK_EXPIRE", timedelta(weeks=1)
    )

    Task.objects.filter(
        events__status__in={Task.FAILED, Task.RETRY},
        status__in={Task.FAILED, Task.SUCCEED},
        updated_at__lte=troubled_task_expire,
    ).delete()

    Task.objects.exclude(events__status__in={Task.FAILED, Task.RETRY}).filter(
        status=Task.SUCCEED, updated_at__lte=succeed_task_expire
    ).delete()
