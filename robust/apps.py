from django.apps import AppConfig

__all__ = ("RobustConfig",)


class RobustConfig(AppConfig):
    name = "robust"

    def ready(self) -> None:
        # noinspection PyUnresolvedReferences
        from . import receivers  # NOQA
