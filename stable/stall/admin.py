from django.contrib import admin

from barn.admin import AbstractScheduleAdmin, AbstractTaskAdmin
from stable.stall.models import SomeSchedule, SomeTask


@admin.register(SomeSchedule)
class SomeScheduleAdmin(AbstractScheduleAdmin):
    list_display = ("id", "max_attempts", "is_active", "cron", "next_run_at")


@admin.register(SomeTask)
class SomeTaskAdmin(AbstractTaskAdmin):
    list_display = ("id",  "attempt", "max_attempts", "run_at", "is_processed", "is_success")

