import logging
from datetime import UTC, datetime
from datetime import timedelta

from django.core.validators import MinValueValidator
from django.db import models, transaction
from django.utils import timezone

from barn.decorators import task

log = logging.getLogger(__name__)


# from stable.stall.tasks import some_task
# some_task.delay()
# some_task.apply_async(countdown=10)  # secs
@task
def some_task(attempt: int = 1, max_attempts: int = 5) -> str:
    log.info("some_task: attempt=%r, max_attempts=%r", attempt, max_attempts)
    try:
        with transaction.atomic():
            raise RuntimeError()
    except RuntimeError:
        if attempt < max_attempts:
            attempt += 1
            some_task.apply_async(
                kwargs=dict(
                    attempt=attempt,
                    max_attempts=max_attempts,
                ),
                countdown=attempt,
            )
        raise
