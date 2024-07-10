from datetime import timedelta

from django.core.validators import MinValueValidator
from django.db import models, transaction
from django.utils import timezone

from barn.models import AbstractSchedule, AbstractTask


class SomeSchedule(AbstractSchedule):
    max_attempts = models.IntegerField()

    def process(self) -> None:
        SomeTask.objects.create(max_attempts=self.max_attempts)


class SomeTask(AbstractTask):
    attempt = models.IntegerField(
        default=1,
        validators=[MinValueValidator(1)]
    )
    max_attempts = models.IntegerField(
        default=1,
        validators=[MinValueValidator(1)],
    )

    def process(self) -> None:
        try:
            with transaction.atomic():
                raise RuntimeError()
        except RuntimeError:
            if self.attempt < self.max_attempts:
                attempt = self.attempt + 1
                SomeTask.objects.create(
                    attempt=attempt,
                    max_attempts=self.max_attempts,
                    run_at=timezone.now() + timedelta(seconds=attempt)
                )
            raise

