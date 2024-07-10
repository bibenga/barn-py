import json

from django.contrib import admin
from django.utils import timezone
from django.utils.safestring import mark_safe

from .models import Schedule, Task

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
    pretty_json_field = None


class AbstractScheduleAdmin(admin.ModelAdmin):
    list_display = ("id", "is_active", "cron", "next_run_at")
    list_filter = ("is_active",)
    ordering = ("-next_run_at",)
    date_hierarchy = "next_run_at"


class AbstractTaskAdmin(admin.ModelAdmin):
    list_display = ("id", "run_at", "is_processed", "is_success")
    list_filter = ("is_processed", "is_success")
    ordering = ("-run_at",)
    date_hierarchy = "run_at"


@admin.register(Schedule)
class ScheduleAdmin(AbstractScheduleAdmin):
    list_display = ("id", "name", "func", "is_active", "cron", "next_run_at")
    search_fields = ("name", "func")
    fields = ("name", "func", "args",  "is_active", "cron", "next_run_at", "last_run_at")
    readonly_fields = ()

    if pretty_json_field is not None:
        fields = list(fields)
        fields.insert(fields.index("args") + 1, "args_pretty")
        readonly_fields += ("args_pretty",)

        def args_pretty(self, instance):
            return pretty_json_field(instance.args)
        args_pretty.short_description = "Args (pretty json)"


@admin.register(Task)
class TaskAdmin(AbstractTaskAdmin):
    list_display = ("id", "func", "run_at", "is_processed", "is_success")
    search_fields = ("func",)
    date_hierarchy = "run_at"
    fields = ("func", "args", "run_at", "is_processed", "started_at",
              "finished_at", "is_success", "result", "error")
    readonly_fields = ()
    actions = ("rerun_task",)

    @admin.action(description="Rerun tasks")
    def rerun_task(self, request, queryset):
        run_at = timezone.now()
        tasks = [Task(func=t.func, args=t.args, run_at=run_at) for t in queryset if t.is_processed]
        if tasks:
            Task.objects.bulk_create(tasks)
            self.message_user(request, f"{len(tasks)} tasks are created")
        else:
            self.message_user(request, f"No tasks are created")

    if pretty_json_field is not None:
        fields = list(fields)
        fields.insert(fields.index("args") + 1, "args_pretty")
        fields.insert(fields.index("result") + 1, "result_pretty")
        readonly_fields += ("args_pretty", "result_pretty")

        def args_pretty(self, instance):
            return pretty_json_field(instance.args)
        args_pretty.short_description = "Args (pretty json)"

        def result_pretty(self, instance):
            return pretty_json_field(instance.result)
        result_pretty.short_description = "Result (pretty json)"
