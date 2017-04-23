import contextlib

from django.conf import settings
from django.db import transaction

__all__ = ('on_commit',)


def on_commit(fn, *args, **kwargs):
    if settings.ROBUST_ALWAYS_EAGER:
        fn(*args, **kwargs)
    else:
        transaction.on_commit(lambda: fn(*args, **kwargs))


@contextlib.contextmanager
def atomic():
    with transaction.atomic():
        yield
