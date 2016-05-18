from django.conf import settings
from django.core.signals import setting_changed
from django.db import models, connection
from django.dispatch import receiver
from django.utils import timezone

from .models import Task, RateLimitRun, TaskManager
from .signals import task_started


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
