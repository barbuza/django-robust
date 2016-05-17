from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import connection, models
from django.dispatch import receiver
from django.utils import timezone

from .exceptions import TaskTransactionError
from .signals import task_started


def count_when(q=None, **kwargs):
    """
    :type q: django.db.models.Q
    :rtype django.db.models.Sum
    """
    if q is None:
        q = models.Q(**kwargs)
    return models.Sum(models.Case(
        models.When(q, then=models.Value(1)),
        default=models.Value(0),
        output_field=models.BigIntegerField()
    ))


class TaskManager(models.Manager):
    def next(self, limit=1):
        """
        lock and return next {limit} unlocked tasks

        :type limit: int
        :rtype: list[Task]
        """
        if not connection.in_atomic_block:
            raise TaskTransactionError('Task.objects.next() must be used inside transaction')

        avoid_tags = []
        limits = getattr(settings, 'ROBUST_RATE_LIMIT', {})
        runtime = timezone.now()
        if limits:
            window = max([duration for count, duration in limits.values()])
            aggs = {}
            for tag, (_, duration) in limits.items():
                aggs[tag] = count_when(tag=tag, created_at__gte=runtime - duration)
            data = RateLimitRun.objects \
                .filter(created_at__gte=runtime - window) \
                .aggregate(**aggs)
            for tag, (count, _) in limits.items():
                if data[tag] >= count:
                    avoid_tags.append(tag)

        query = '''
        SELECT * FROM {}
        WHERE status IN (%s, %s) AND (eta IS NULL OR eta <= %s) AND NOT tags && %s
        ORDER BY {}
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        '''.format(Task._meta.db_table, Task._meta.pk.name)

        return list(self.raw(query, params=[
            Task.PENDING, Task.RETRY, timezone.now(), avoid_tags, limit
        ]))


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
        for idx, event in enumerate(self.events.all()):
            if idx == 0:
                action = 'created'
            else:
                action = event.get_status_display()
            items.append('{} {}'.format(event.created_at, action))
        return '\n'.join(items)

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
