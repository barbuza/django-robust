from django.dispatch import Signal

task_signal_args = {"pk", "name", "payload", "raw_payload", "tags"}

task_started = Signal(providing_args=task_signal_args)
task_succeed = Signal(providing_args=task_signal_args)
task_failed = Signal(providing_args=task_signal_args)
task_retry = Signal(providing_args=task_signal_args)
