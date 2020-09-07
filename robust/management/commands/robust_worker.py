import argparse
import multiprocessing
from typing import Any

from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string


class Command(BaseCommand):
    requires_system_checks = True
    can_import_settings = True

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        super(Command, self).add_arguments(parser)
        parser.add_argument(
            "--concurrency", default=multiprocessing.cpu_count(), type=int
        )
        parser.add_argument("--bulk", default=1, type=int)
        parser.add_argument("--limit", default=None, type=int)
        parser.add_argument("--runner", default="robust.runners.SimpleRunner")
        parser.add_argument("--beat", default=False, action="store_true")

    def handle(
        self,
        concurrency: int,
        bulk: int,
        limit: int,
        runner: str,
        beat: bool,
        **_kwargs: Any
    ) -> None:
        from ...worker import run_worker

        runner_cls = import_string(runner)
        run_worker(concurrency, bulk, limit, runner_cls, beat)
