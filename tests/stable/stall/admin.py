from django.contrib import admin

from barn.admin import AbstractScheduleAdmin, AbstractTaskAdmin

from .models import SomeSchedule, SomeTask


@admin.register(SomeSchedule)
class SomeScheduleAdmin(AbstractScheduleAdmin):
    list_display = ("id", "max_attempts", "is_active", "next_run_at", "interval", "cron")


@admin.register(SomeTask)
class SomeTaskAdmin(AbstractTaskAdmin):
    list_display = ("id", "correlation_id", "attempt", "max_attempts",
                    "run_at", "colored_status")
    search_fields = ("correlation_id",)
