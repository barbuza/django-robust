import multiprocessing

from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string


class Command(BaseCommand):
    requires_system_checks = True
    can_import_settings = True

    def add_arguments(self, parser):
        """
        :type parser: argparse.ArgumentParser
        """
        super(Command, self).add_arguments(parser)
        parser.add_argument('--concurrency', default=multiprocessing.cpu_count(), type=int)
        parser.add_argument('--bulk', default=1, type=int)
        parser.add_argument('--limit', default=None, type=int)
        parser.add_argument('--runner', default='robust.runners.SimpleRunner')
        parser.add_argument('--beat', default=False, action='store_true')

    def handle(self, *args, **options):
        assert options['bulk']
        from ...worker import run_worker
        runner_cls = import_string(options['runner'])
        run_worker(
            options['concurrency'], options['bulk'], options['limit'],
            runner_cls, options['beat']
        )
