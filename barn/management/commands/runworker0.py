from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...worker import Worker


class Command(BaseCommand):
    help = "Worker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--with_autoreload",
            action="store_true",
            dest="use_reloader",
        )
        parser.add_argument(
            "--interval",
            type=int,
            default=5,
        )

    def handle(self, *args, **options):
        use_reloader = options["use_reloader"]
        if use_reloader:
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, **options):
        worker = Worker(
            use_signals=not options["use_reloader"],
            interval=options["interval"]
        )
        worker.run()