from django.core.management.base import BaseCommand


class Command(BaseCommand):
    requires_system_checks = True
    can_import_settings = True

    def handle(self, *args, **options):
        from ...beat import run_beat
        run_beat()
