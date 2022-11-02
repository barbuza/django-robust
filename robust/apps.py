from django.apps import AppConfig

__all__ = ("RobustConfig",)


class RobustConfig(AppConfig):
    name = "robust"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        # noinspection PyUnresolvedReferences
        from . import receivers  # NOQA
