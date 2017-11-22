from typing import List

from django.conf import settings
from django.core.signals import setting_changed
from django.db import connection, models
from django.dispatch import receiver
from django.utils import timezone

from .models import RateLimitRun, Task, TaskManager
from .signals import task_started


@receiver(signal=models.signals.pre_save, sender=Task)
def task_fields_defaults(instance: Task, **kwargs) -> None:
    if instance.payload is None:
        instance.payload = {}
    if instance.tags is None:
        instance.tags = []


@receiver(signal=models.signals.post_save, sender=Task)
def create_log_record(instance: Task, created: bool, **kwargs) -> None:
    if getattr(settings, 'ROBUST_LOG_EVENTS', True):
        instance.events.create(
            status=instance.status,
            eta=instance.eta,
            created_at=instance.created_at if created else instance.updated_at
        )


def _notify_change() -> None:
    with connection.cursor() as cursor:
        cursor.execute('NOTIFY robust')


@receiver(signal=models.signals.post_save, sender=Task)
def notify_change(instance: Task, **kwargs) -> None:
    if instance.status in (Task.PENDING, Task.RETRY):
        if connection.in_atomic_block:
            on_commit_task = (set(connection.savepoint_ids), _notify_change)
            if on_commit_task not in connection.run_on_commit:
                connection.on_commit(_notify_change)
        else:
            _notify_change()


@receiver(signal=task_started)
def update_ratelimit(tags: List[str], **kwargs) -> None:
    if tags:
        runtime = timezone.now()
        RateLimitRun.objects.using('robust_ratelimit').bulk_create(
            [RateLimitRun(tag=tag, created_at=runtime) for tag in tags]
        )


@receiver(signal=setting_changed)
def reset_query_cache(setting: str, **kwargs) -> None:
    if setting == 'ROBUST_RATE_LIMIT':
        TaskManager.reset_query_cache()
