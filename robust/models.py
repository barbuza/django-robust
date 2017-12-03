import copy
import sys
import threading
import traceback
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, cast

from django.conf import settings
from django.contrib.postgres.fields import ArrayField, JSONField
from django.db import connection, models
from django.utils import timezone

from .exceptions import TaskTransactionError


class TaskQuerySet(models.QuerySet):
    def with_fails(self) -> 'TaskQuerySet':
        qs = self.filter(events__status__in=Task.TROUBLED_STATUSES).distinct()
        return cast(TaskQuerySet, qs)

    def without_fails(self) -> 'TaskQuerySet':
        qs = self.exclude(events__status__in=Task.TROUBLED_STATUSES).distinct()
        return cast(TaskQuerySet, qs)


class TaskManager(models.Manager):
    _query_cache_lock = threading.Lock()
    _query_cache = None
    _query_limits = None

    def get_queryset(self) -> 'TaskQuerySet':
        return TaskQuerySet(self.model, using=self._db)

    @classmethod
    def _reset_query_cache(cls) -> None:
        with cls._query_cache_lock:
            cls._query_cache = None
            cls._params_cache = None

    @classmethod
    def _compile_query(cls) -> None:
        rate_limit = getattr(settings, 'ROBUST_RATE_LIMIT', None)
        if rate_limit:
            array_items = ['''
                (CASE WHEN
                  SUM(
                    CASE WHEN created_at >= %s AND tag = %s THEN 1 ELSE 0 END
                  ) >= %s THEN %s
                  ELSE NULL END)
            '''] * len(rate_limit)

            ratelimit_query = f'''
            SELECT array_remove(ARRAY[{", ".join(array_items)}], NULL)
            FROM {RateLimitRun._meta.db_table} WHERE created_at >= %s
            '''

            query = f'''
            SELECT * FROM {Task._meta.db_table}
            WHERE status IN (%s, %s) AND (eta IS NULL OR eta <= %s)
              AND NOT tags && ({ratelimit_query})
            ORDER BY {Task._meta.pk.name}
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            '''

        else:
            query = f'''
            SELECT * FROM {Task._meta.db_table}
            WHERE status IN (%s, %s) AND (eta IS NULL OR eta <= %s)
            ORDER BY {Task._meta.pk.name}
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            '''

        cls._query_cache = query
        cls._query_limits = copy.deepcopy(rate_limit)

    @classmethod
    def _query_params(cls, limit: int) -> list:
        runtime = timezone.now()
        params = [Task.PENDING, Task.RETRY, runtime]
        rate_limit = cls._query_limits
        if rate_limit:
            for tag, (count, duration) in rate_limit.items():
                params.extend((runtime - duration, tag, count, tag))
            window = max([duration for count, duration in rate_limit.values()])
            params.append(runtime - window)
        params.append(limit)
        return params

    def next(self, limit: int = 1) -> List['Task']:
        """
        lock and return next {limit} unlocked tasks
        """
        if not connection.in_atomic_block:
            raise TaskTransactionError('Task.objects.next() must be used '
                                       'inside transaction')

        with self._query_cache_lock:
            if not self._query_cache:
                self._compile_query()

            query = self._query_cache
            params = self._query_params(limit)

        return list(self.raw(query, params=params))


class Task(models.Model):
    PENDING = 0
    RETRY = 1
    SUCCEED = 2
    FAILED = 3

    TROUBLED_STATUSES = [RETRY, FAILED]

    STATUS_CHOICES = [
        (PENDING, 'pending'),
        (RETRY, 'retry'),
        (SUCCEED, 'succeed'),
        (FAILED, 'failed')
    ]

    status = models.PositiveSmallIntegerField(choices=STATUS_CHOICES,
                                              db_index=True, default=PENDING)
    retries = models.IntegerField(null=True)
    name = models.TextField()
    tags = ArrayField(models.TextField())
    payload = JSONField()
    traceback = models.TextField(null=True)
    eta = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = TaskManager()

    def _format_traceback(self) -> Optional[str]:
        etype, value, tb = sys.exc_info()
        try:
            if etype and value and tb:
                return ''.join(traceback.format_exception(etype, value, tb))
            return None
        finally:
            del tb

    def mark_retry(self, eta: Optional[datetime] = None,
                   delay: Optional[timedelta] = None,
                   trace: Optional[str] = None) -> None:
        """
        mark task for retry with given {eta} or {delay}
        """
        if delay is not None:
            eta = timezone.now() + delay
        self.eta = eta
        self.status = self.RETRY
        self.traceback = trace
        self.save(update_fields={'eta', 'status', 'traceback', 'retries',
                                 'updated_at'})

    def mark_succeed(self) -> None:
        """
        mark task as succeed
        """
        self.status = self.SUCCEED
        self.traceback = None
        self.save(update_fields={'status', 'traceback', 'updated_at'})

    def mark_failed(self) -> None:
        """
        mark task as failed
        """
        self.status = self.FAILED
        self.traceback = self._format_traceback()
        self.save(update_fields={'status', 'traceback', 'updated_at'})

    @property
    def log(self) -> str:
        """
        task log
        """
        items = []
        for idx, event in enumerate(self.log_events):
            if idx == 0:
                action = 'created'
            else:
                action = event.get_status_display()
            items.append(f'{event.created_at} {action}')
        return '\n'.join(items)

    @property
    def log_events(self) -> Iterable['TaskEvent']:
        return self.events.order_by('pk')

    def __repr__(self) -> str:
        chunks = [self.name, f'#{self.pk}', self.get_status_display()]
        if self.eta:
            chunks.insert(2, str(self.eta))
        return f'<Task {" ".join(chunks)}>'

    __str__ = __repr__

    class Meta:
        index_together = [('status', 'eta')]


class TaskEvent(models.Model):
    task = models.ForeignKey(Task, related_name='events',
                             on_delete=models.CASCADE)
    created_at = models.DateTimeField()
    status = models.PositiveSmallIntegerField(choices=Task.STATUS_CHOICES)
    eta = models.DateTimeField(blank=True, null=True)
    traceback = models.TextField(null=True)


class RateLimitRun(models.Model):
    created_at = models.DateTimeField(db_index=True)
    tag = models.TextField()
