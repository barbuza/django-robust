from django.dispatch import Signal


task_started = Signal()
task_succeed = Signal()
task_failed = Signal()
task_retry = Signal()
beat_tick = Signal()
