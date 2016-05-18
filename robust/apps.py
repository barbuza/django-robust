from django.apps import AppConfig

__all__ = ('RobustConfig',)


class RobustConfig(AppConfig):
    name = 'robust'

    def ready(self):
        # noinspection PyUnresolvedReferences
        from . import receivers  # NOQA
