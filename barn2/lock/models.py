from django.db import models


class Lock(models.Model):
    name = models.TextField(primary_key=True)
    locked_at = models.DateTimeField(null=True, blank=True)
    owner = models.TextField(null=True, blank=True)

    def __str__(self):
        return self.name


class Schedule(models.Model):
    name = models.TextField()
    is_active = models.BooleanField(default=True)
    cron = models.TextField(null=True, blank=True)
    next_run_at = models.DateTimeField(null=True, blank=True)
    last_run_at = models.DateTimeField(null=True, blank=True)
    func = models.TextField()
    args = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.name}"


class Task(models.Model):
    created = models.DateTimeField(auto_now=True)
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


class Queue(models.Model):
    created = models.DateTimeField(auto_now=True)
    payload = models.JSONField(null=True)

    def __str__(self):
        return f"message:{self.id}"

    # class Meta:
    #     abstract = True
