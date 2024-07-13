import logging

from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from django.utils.module_loading import import_string
from django.utils.translation import gettext_lazy

log = logging.getLogger(__name__)


def validate_cron(value):
    try:
        from croniter import croniter
    except ImportError:
        raise ValidationError("croniter is not installed")
    try:
        croniter(value)
    except (ValueError, TypeError) as err:
        raise ValidationError(
            "%(value)r is invalid cron value",
            params={"value": value},
        ) from err


class AbstractSchedule(models.Model):
    is_active = models.BooleanField(default=True)
    next_run_at = models.DateTimeField(null=True, blank=True, db_index=True)
    interval = models.DurationField(null=True, blank=True)
    cron = models.CharField(max_length=200, null=True, blank=True, validators=[validate_cron],
                            help_text="Exactly 5 or 6 columns has to be specified for iterator expression")
    last_run_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return f"schedule:{self.pk}"

    def clean(self) -> None:
        if self.interval and self.cron:
            raise ValidationError("The cron or interval are required")
        super().clean()

    def save(self, *args, **kwargs) -> None:
        if self.interval and self.cron:
            raise ValidationError("The cron or interval are required")
        super().save(*args, **kwargs)

    def process(self) -> None:
        raise NotImplementedError


class TaskStatus(models.TextChoices):
    QUEUED = "Q", gettext_lazy("Queued")
    DONE = "D", gettext_lazy("Done")
    FAILED = "F", gettext_lazy("Failed")


class AbstractTask(models.Model):
    run_at = models.DateTimeField(db_index=True, blank=True)
    status = models.CharField(max_length=1, choices=TaskStatus.choices, default=TaskStatus.QUEUED)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return f"task:{self.pk}"

    # def clean(self) -> None:
    #     self.run_at = self.run_at or timezone.now()
    #     return super().clean()

    def save(self, *args, **kwargs) -> None:
        self.run_at = self.run_at or timezone.now()
        super().save(*args, **kwargs)

    def process(self) -> None:
        raise NotImplementedError


class Schedule(AbstractSchedule):
    name = models.CharField(max_length=100, null=True, blank=True)
    func = models.CharField(max_length=1000)
    args = models.JSONField(null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.name}"

    # def clean(self) -> None:
    #     self.name = self.name or self.func

    def save(self, *args, **kwargs) -> None:
        self.name = self.name or self.func
        super().save(*args, **kwargs)

    def process(self) -> None:
        task = Task.objects.create(run_at=self.next_run_at, func=self.func, args=self.args)
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
