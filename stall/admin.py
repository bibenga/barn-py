from django.contrib import admin

from barn.admin import AbstractScheduleAdmin, AbstractTaskAdmin
from stall.models import Schedule1, Schedule2, Task1, Task2


@admin.register(Schedule1)
class Schedule1Admin(AbstractScheduleAdmin):
    list_display = ("id", "arg1", "arg2", "is_active", "cron", "next_run_at")


@admin.register(Task1)
class Task1Admin(AbstractTaskAdmin):
    list_display = ("id",  "arg1", "arg2", "created", "is_processed", "is_success")


@admin.register(Schedule2)
class Schedule2Admin(AbstractScheduleAdmin):
    list_display = ("id", "arg1", "is_active", "cron", "next_run_at")


@admin.register(Task2)
class Task2Admin(AbstractTaskAdmin):
    list_display = ("id", "arg1", "created", "is_processed", "is_success")
