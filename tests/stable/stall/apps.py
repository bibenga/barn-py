from django.apps import AppConfig


class ShopConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "tests.stable.stall"

    def ready(self):
        from barn.bus import PgBus
        from .models import SomeSchedule, SomeTask
        PgBus.connect(SomeTask, SomeSchedule)
