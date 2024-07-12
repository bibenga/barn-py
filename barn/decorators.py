import logging
from datetime import datetime, timedelta
from functools import partial, wraps

from django.db import transaction
from django.utils import timezone

from .conf import Conf
from .models import Task

log = logging.getLogger(__name__)


def task(func):
    @wraps(func)
    def _delay(**kwargs) -> Task:
        return apply_async(func, kwargs=kwargs)

    @wraps(func)
    def _apply_async(
        kwargs: dict | None = None,
        countdown: timedelta | int | float | None = None,
        eta: datetime | None = None,
    ) -> Task:
        return apply_async(
            func,
            kwargs=kwargs,
            countdown=countdown,
            eta=eta,
        )

    func.delay = _delay
    func.apply_async = _apply_async
    return func


def apply_async(
    func,
    kwargs: dict | None = None,
    countdown: timedelta | int | float | None = None,
    eta: datetime | None = None,
) -> Task:
    func = f"{func.__module__}.{func.__name__}"
    run_at = None
    if countdown:
        if isinstance(countdown, timedelta):
            run_at = timezone.now() + countdown
        else:
            run_at = timezone.now() + timedelta(seconds=countdown)
    elif eta:
        run_at = eta

    task = Task.objects.create(func=func, args=kwargs, run_at=run_at)
    log.info("the task %s is queued", task.pk)

    if Conf.TASK_SYNC:
        if run_at:
            raise RuntimeError("A task cannot be executed in eager mode")

        def _call() -> None:
            log.warning("run the task %s in sync mode", task)
            from .worker import Worker
            worker = Worker(Task)
            worker.sync_call_task(task)

        transaction.on_commit(_call)

    return task


