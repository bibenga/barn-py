import logging
import time

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from barn.decorators import task
from barn.models import Task

log = logging.getLogger(__name__)


# from barn.models import Task, TaskStatus
# from django.db.models import Q
# from django.utils import timezone
# Task.objects.filter(run_at__lt=timezone.now(), status=TaskStatus.QUEUED).order_by("run_at", "id").explain()
# Task.objects.filter(run_at__lt=timezone.now(), status__in=[TaskStatus.DONE, TaskStatus.FAILED]).explain()
# Task.objects.filter(Q(run_at__lt=timezone.now()), Q(status=TaskStatus.DONE) | Q(status=TaskStatus.FAILED)).explain()
# Task.objects.filter(run_at__lt=timezone.now(), status=TaskStatus.DONE).explain()

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "-s",
            "--simple",
            dest="simple",
            action="store_true",
        )

        parser.add_argument(
            "-t",
            "--task",
            dest="task",
            default=1000,
            type=int
        )

    def handle(self, *args, **options):
        simple = options["simple"]
        count = options["task"]
        if simple:
            started = time.perf_counter()
            for i in range(count):
                with transaction.atomic():
                    simple_task.delay(i=i, count=count, simple=simple)
            duration = time.perf_counter() - started
            log.info("pushed %d tasks in %.4fs: %.4f rps", count, duration, count/duration)
        else:
            started = time.perf_counter()
            tasks = [
                Task(
                    func=f"{simple_task.__module__}.{simple_task.__name__}",
                    args=dict(i=i, count=count, simple=simple),
                    run_at=timezone.now(),
                )
                for i in range(count)
            ]
            with transaction.atomic():
                Task.objects.bulk_create(tasks, batch_size=1000)
            duration = time.perf_counter() - started
            log.info("pushed %d tasks in %.4fs: %.4f rps", count, duration, count/duration)


@task
def simple_task(**kwargs) -> dict:
    return kwargs
