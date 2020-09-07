from typing import Any, List

from django.conf import settings
from django.db import connection, models
from django.dispatch import receiver
from django.utils import timezone

from .models import Task, save_tag_run
from .signals import task_started


@receiver(signal=models.signals.pre_save, sender=Task)
def task_fields_defaults(instance: Task, **_kwargs: Any) -> None:
    if instance.payload is None:
        instance.payload = {}
    if instance.tags is None:
        instance.tags = []


@receiver(signal=models.signals.post_save, sender=Task)
def create_log_record(instance: Task, created: bool, **_kwargs: Any) -> None:
    if getattr(settings, "ROBUST_LOG_EVENTS", True):
        instance.events.create(
            status=instance.status,
            eta=instance.eta,
            created_at=instance.created_at if created else instance.updated_at,
        )


def _notify_change() -> None:
    with connection.cursor() as cursor:
        cursor.execute("NOTIFY robust")


@receiver(signal=models.signals.post_save, sender=Task)
def notify_change(instance: Task, **_kwargs: Any) -> None:
    if instance.status in (Task.PENDING, Task.RETRY):
        if connection.in_atomic_block:
            on_commit_task = (set(connection.savepoint_ids), _notify_change)
            if on_commit_task not in connection.run_on_commit:
                connection.on_commit(_notify_change)
        else:
            _notify_change()


@receiver(signal=task_started)
def update_ratelimit(tags: List[str], **_kwargs: Any) -> None:
    if tags:
        runtime = timezone.now()
        for tag in tags:
            save_tag_run(tag, runtime.replace(microsecond=0))
