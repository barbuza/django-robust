from django.dispatch import Signal

task_started = Signal(providing_args=['name', 'payload', 'tags'])
