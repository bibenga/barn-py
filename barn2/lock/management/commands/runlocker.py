from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...locker import Locker


class Command(BaseCommand):
    help = "Locker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--with_autoreload",
            action="store_true",
            dest="use_reloader",
        )

    def handle(self, *args, **options):
        use_reloader = options["use_reloader"]
        if use_reloader:
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, **options):
        locker = Locker(
            "barn",
            use_signals=not options["use_reloader"],
        )
        locker.run()
