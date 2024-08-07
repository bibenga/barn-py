import json

from django.contrib import admin
from django.db import transaction
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from .models import Schedule, Task, TaskStatus

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
    list_display = ("id", "is_active", "next_run_at", "interval", "cron")
    list_filter = ("is_active",)


class AbstractTaskAdmin(admin.ModelAdmin):
    list_display = ("id", "run_at", "colored_status")
    list_filter = ("status", "run_at")

    @admin.display(empty_value="unknown", ordering="status")
    def colored_status(self, obj):
        colors = {
            TaskStatus.DONE: "green",
            TaskStatus.FAILED: "var(--error-fg)",
        }
        if obj.status in colors:
            return format_html(
                "<span style=\"color: {}\">{}</div>",
                colors[obj.status],
                obj.get_status_display(),
            )
        else:
            return obj.get_status_display()


@admin.register(Schedule)
class ScheduleAdmin(AbstractScheduleAdmin):
    list_display = ("id", "name", "func", "is_active", "next_run_at", "interval", "cron")
    search_fields = ("name", "func")
    ordering = ("name",)
    fields = ("name", "func", "args",  "is_active",
              "next_run_at", "interval", "cron", "last_run_at")
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
    list_display = ("id", "func", "run_at", "colored_status")
    search_fields = ("func",)
    fields = ("func", "args", "run_at", "status", "started_at",
              "finished_at", "result", "error")
    readonly_fields = ()
    actions = ("rerun_task",)

    @admin.action(description="Rerun tasks")
    @transaction.atomic
    def rerun_task(self, request, queryset):
        run_at = timezone.now()
        queryset = queryset.filter(status=TaskStatus.FAILED)
        if not queryset.exists():
            self.message_user(request, f"Tasks in status {TaskStatus.FAILED} weren't found")
        else:
            for t in queryset:
                nt = Task.objects.create(func=t.func, args=t.args, run_at=run_at)
                self.message_user(request, f"The task {t.pk} is cloned as the task {nt.pk}")

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
