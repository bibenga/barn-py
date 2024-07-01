import json

from django.contrib import admin
from django.utils.safestring import mark_safe

from .models import Schedule, Task


class AbstractScheduleAdmin(admin.ModelAdmin):
    list_display = ("id", "is_active", "cron", "next_run_at")
    list_filter = ("is_active",)
    ordering = ("-next_run_at",)
    date_hierarchy = "next_run_at"


class AbstractTaskAdmin(admin.ModelAdmin):
    list_display = ("id", "run_at", "is_processed", "is_success")
    list_filter = ("is_processed", "is_success")
    ordering = ("-run_at",)
    search_fields = ("func",)
    date_hierarchy = "run_at"


@admin.register(Schedule)
class ScheduleAdmin(AbstractScheduleAdmin):
    list_display = ("id", "name", "func", "is_active", "cron", "next_run_at")
    search_fields = ("name", "func")
    fields = ("name", "func", "args", "args_pretty",
              "is_active", "cron", "next_run_at", "last_run_at")
    readonly_fields = ("args_pretty", )

    def args_pretty(self, instance):
        """Function to display pretty version of our data"""
        return pretty_json_field(instance.args)

    args_pretty.short_description = "args"


@admin.register(Task)
class TaskAdmin(AbstractTaskAdmin):
    list_display = ("id", "func", "run_at", "is_processed", "is_success")
    search_fields = ("func",)
    date_hierarchy = "run_at"
    fields = ("func", "args", "args_pretty", "run_at", "is_processed",
              "started_at", "finished_at", "is_success", "result", "result_pretty", "error")
    readonly_fields = ("args_pretty", "result_pretty")

    def args_pretty(self, instance):
        """Function to display pretty version of our data"""
        return pretty_json_field(instance.args)

    args_pretty.short_description = "args"

    def result_pretty(self, instance):
        """Function to display pretty version of our data"""
        return pretty_json_field(instance.result)

    result_pretty.short_description = "result"


try:
    from pygments import highlight
    from pygments.formatters import HtmlFormatter
    from pygments.lexers import JsonLexer

    def pretty_json_field(payload):
        """Function to display pretty version of our data"""
        response = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
        formatter = HtmlFormatter()
        response = highlight(response, JsonLexer(), formatter)
        style = "<style>" + formatter.get_style_defs() + "</style><br>"
        return mark_safe(style + response)
except ImportError:
    def pretty_json_field(payload):
        return payload
