import logging

from django.db import transaction

from barn.decorators import task

log = logging.getLogger(__name__)


# from tests.stable.stall.tasks import simple_task, task_with_retry
# simple_task.delay()
# task_with_retry.delay()
# task_with_retry.apply_async(countdown=10)  # secs

@task
def task_with_retry(attempt: int = 1, max_attempts: int = 5) -> str:
    log.info("some_task: attempt=%r, max_attempts=%r", attempt, max_attempts)
    try:
        with transaction.atomic():
            raise RuntimeError()
    except RuntimeError:
        if attempt < max_attempts:
            attempt += 1
            task_with_retry.apply_async(
                kwargs=dict(
                    attempt=attempt,
                    max_attempts=max_attempts,
                ),
                countdown=attempt,
            )
        raise


@task
def simple_task(attempt: int = 1, max_attempts: int = 5) -> str:
    log.info("some_task: attempt=%r, max_attempts=%r", attempt, max_attempts)
    return "ok"
