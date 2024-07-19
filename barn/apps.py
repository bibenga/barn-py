from django.apps import AppConfig


class BarnConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "barn"

    def ready(self):
        from .models import Schedule, Task
        from .bus import PgBus
        PgBus.connect(Task, Schedule)
