import copy
import threading

from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import connection, models
from django.dispatch import receiver
from django.test.utils import setting_changed
from django.utils import timezone

from .exceptions import TaskTransactionError
from .signals import task_started


class TaskManager(models.Manager):
    _query_cache_lock = threading.Lock()
    _query_cache = None
    _query_limits = None

    @classmethod
    def reset_query_cache(cls):
        with cls._query_cache_lock:
            cls._query_cache = None
            cls._params_cache = None

    @classmethod
    def _compile_query(cls):
        rate_limit = getattr(settings, 'ROBUST_RATE_LIMIT', None)
        if rate_limit:
            array_items = ['''
                (CASE WHEN
                  SUM(
                    CASE WHEN created_at >= %s AND tag = %s THEN 1 ELSE 0 END
                  ) >= %s THEN %s
                  ELSE NULL END)
            '''] * len(rate_limit)

            ratelimit_query = '''
            SELECT array_remove(ARRAY[{}], NULL)
            FROM {} WHERE created_at >= %s
            '''.format(','.join(array_items), RateLimitRun._meta.db_table)

            query = '''
            SELECT * FROM {}
            WHERE status IN (%s, %s) AND (eta IS NULL OR eta <= %s) AND NOT tags && ({})
            ORDER BY {}
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            '''.format(Task._meta.db_table, ratelimit_query, Task._meta.pk.name)

        else:
            query = '''
            SELECT * FROM {}
            WHERE status IN (%s, %s) AND (eta IS NULL OR eta <= %s)
            ORDER BY {}
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            '''.format(Task._meta.db_table, Task._meta.pk.name)

        cls._query_cache = query
        cls._query_limits = copy.deepcopy(rate_limit)

    @classmethod
    def _query_params(cls, limit):
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

    def next(self, limit=1):
        """
        lock and return next {limit} unlocked tasks

        :type limit: int
        :rtype: list[Task]
        """
        with self._query_cache_lock:
            if not self._query_cache:
                self._compile_query()

            query = self._query_cache
            params = self._query_params(limit)

        if not connection.in_atomic_block:
            raise TaskTransactionError('Task.objects.next() must be used inside transaction')

        return list(self.raw(query, params=params))


class Task(models.Model):
    PENDING = 0
    RETRY = 1
    SUCCEED = 2
    FAILED = 3

    STATUS_CHOICES = [
        (PENDING, 'pending'),
        (RETRY, 'retry'),
        (SUCCEED, 'succeed'),
        (FAILED, 'failed')
    ]

    status = models.PositiveSmallIntegerField(choices=STATUS_CHOICES, db_index=True, default=PENDING)
    name = models.TextField()
    tags = ArrayField(models.TextField())
    payload = JSONField()
    eta = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = TaskManager()

    def mark_retry(self, eta=None, delay=None):
        """
        mark task for retry with given {eta} or {delay}

        :type eta: datetime.datetime
        :type delay: datetime.timedelta
        """
        if delay is not None:
            eta = timezone.now() + delay
        self.eta = eta
        self.status = self.RETRY
        self.save()

    def mark_succeed(self):
        """
        mark task as succeed
        """
        self.status = self.SUCCEED
        self.save()

    def mark_failed(self):
        """
        mark task as failed
        """
        self.status = self.FAILED
        self.save()

    @property
    def log(self):
        """
        task log

        :rtype str
        """
        items = []
        for idx, event in enumerate(self.log_events):
            if idx == 0:
                action = 'created'
            else:
                action = event.get_status_display()
            items.append('{} {}'.format(event.created_at, action))
        return '\n'.join(items)

    @property
    def log_events(self):
        return self.events.order_by('pk')

    def __repr__(self):
        chunks = [self.name, '#{}'.format(self.pk), self.get_status_display()]
        if self.eta:
            chunks.insert(2, str(self.eta))
        return '<Task {}>'.format(' '.join(chunks))

    __str__ = __unicode__ = __repr__

    class Meta:
        index_together = [('status', 'eta')]


class TaskEvent(models.Model):
    task = models.ForeignKey(Task, related_name='events')
    created_at = models.DateTimeField()
    status = models.PositiveSmallIntegerField(choices=Task.STATUS_CHOICES)
    eta = models.DateTimeField(blank=True, null=True)


class RateLimitRun(models.Model):
    created_at = models.DateTimeField(db_index=True)
    tag = models.TextField()


@receiver(signal=models.signals.pre_save, sender=Task)
def task_fields_defaults(instance, **kwargs):
    """
    :type instance: Task
    """
    if instance.payload is None:
        instance.payload = {}
    if instance.tags is None:
        instance.tags = []


@receiver(signal=models.signals.post_save, sender=Task)
def create_log_record(instance, created, **kwargs):
    """
    :type instance: Task
    :type created: bool
    """
    if getattr(settings, 'ROBUST_LOG_EVENTS', True):
        instance.events.create(
            status=instance.status,
            eta=instance.eta,
            created_at=instance.created_at if created else instance.updated_at
        )


@receiver(signal=models.signals.post_save, sender=Task)
def notify_change(instance, **kwargs):
    """
    :type instance: Task
    """
    if instance.status in (Task.PENDING, Task.RETRY):
        with connection.cursor() as cursor:
            cursor.execute('NOTIFY robust')


@receiver(signal=task_started)
def update_ratelimit(tags, **kwargs):
    """
    :type tags: list[str]
    """
    if tags:
        runtime = timezone.now()
        RateLimitRun.objects.using('robust_ratelimit').bulk_create(
            [RateLimitRun(tag=tag, created_at=runtime) for tag in tags]
        )


@receiver(signal=setting_changed)
def reset_query_cache(setting, **kwargs):
    """
    :type setting: str
    """
    if setting == 'ROBUST_RATE_LIMIT':
        TaskManager.reset_query_cache()
