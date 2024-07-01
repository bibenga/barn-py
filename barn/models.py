import logging

from croniter import croniter
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.module_loading import import_string
from django.utils import timezone

log = logging.getLogger(__name__)


def validate_cron(value):
    try:
        croniter(value)
    except (ValueError, TypeError) as err:
        raise ValidationError(
            "%(value)r is invalid cron value",
            params={"value": value},
        ) from err


class AbstractSchedule(models.Model):
    is_active = models.BooleanField(default=True)
    cron = models.CharField(max_length=200, null=True, blank=True, validators=[validate_cron],
                            help_text="Exactly 5 or 6 columns has to be specified for iterator expression")
    next_run_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_run_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return f"schedule:{self.pk}"

    def clean(self) -> None:
        if not self.cron and not self.next_run_at:
            raise ValidationError("The cron and/or next_run_at is required")
        return super().clean()

    def process(self) -> None:
        raise NotImplementedError


class AbstractTask(models.Model):
    created = models.DateTimeField(db_index=True, blank=True)
    is_processed = models.BooleanField(default=False)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    is_success = models.BooleanField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return f"task:{self.pk}"

    def clean(self) -> None:
        self.created = self.created or timezone.now()
        return super().clean()

    def process(self) -> None:
        raise NotImplementedError


class Schedule(AbstractSchedule):
    name = models.CharField(max_length=100, null=True, blank=True)
    func = models.CharField(max_length=1000)
    args = models.JSONField(null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.name}"

    def clean(self) -> None:
        self.name = self.name or self.func

    def process(self) -> None:
        task = Task.objects.create(created=self.next_run_at, func=self.func, args=self.args)
        log.info("the task %s is created for schedule %s", task.pk, self.pk)


class Task(AbstractTask):
    func = models.CharField(max_length=1000)
    args = models.JSONField(null=True, blank=True)
    result = models.JSONField(null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.func}"

    def process(self) -> None:
        func = import_string(self.func)
        self.result = func(**(self.args or {}))
