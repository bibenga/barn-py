import json

from django.contrib import admin
from django.utils.safestring import mark_safe

from .models import Lock, Schedule, Task


@admin.register(Lock)
class LockAdmin(admin.ModelAdmin):
    list_display = ("name", "locked_at", "owner")
    ordering = ("name",)
    search_fields = ("name",)


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "func", "is_active", "cron", "next_run_at")
    list_filter = ("is_active",)
    ordering = ("-next_run_at",)
    search_fields = ("name", "func")
    date_hierarchy = "next_run_at"
    readonly_fields = ("args_pretty", )

    def args_pretty(self, instance):
        """Function to display pretty version of our data"""
        return pretty_json_field(instance.args)

    args_pretty.short_description = "args"


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ("id", "func", "created", "is_processed", "is_success")
    list_filter = ("is_processed", "is_success")
    ordering = ("-created",)
    search_fields = ("func",)
    date_hierarchy = "created"
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
