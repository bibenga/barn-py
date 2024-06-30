from datetime import UTC, datetime, timedelta

from croniter import croniter
from django.core.exceptions import ValidationError
from django.db import models


def validate_cron(value):
    try:
        croniter(value)
    except (ValueError, TypeError) as err:
        raise ValidationError(
            "%(value)s invalid cron value",
            params={"value": value},
        ) from err


class Lock(models.Model):
    name = models.CharField(max_length=40, primary_key=True)
    locked_at = models.DateTimeField(null=True, blank=True)
    owner = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self):
        return self.name

    def _is_locked(self) -> bool:
        rotten_ts = datetime.now(UTC) - timedelta(seconds=30)
        return self.locked_at is not None and self.locked_at > rotten_ts
    _is_locked.boolean = True
    is_locked = property(_is_locked)


class Schedule(models.Model):
    name = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    cron = models.TextField(null=True, blank=True, validators=[validate_cron],
                            help_text="Exactly 5 or 6 columns has to be specified for iterator expression")
    next_run_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_run_at = models.DateTimeField(null=True, blank=True)
    func = models.TextField()
    args = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.name}"

    def clean(self) -> None:
        if not self.cron and not self.next_run_at:
            raise ValidationError("The cron and/or next_run_at is required")
        return super().clean()


class Task(models.Model):
    created = models.DateTimeField(auto_now=True, db_index=True)
    func = models.TextField()
    args = models.JSONField(null=True, blank=True)
    is_processed = models.BooleanField(default=False)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    is_success = models.BooleanField(null=True, blank=True)
    result = models.JSONField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)

    def __str__(self):
        return f"{self.func}"
