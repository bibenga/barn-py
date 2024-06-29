from django.conf import settings
from django.utils.decorators import classproperty

class Conf:
    INTERVAL = getattr(settings, "BARN_INTERVAL", 5)  # seconds
    # TASK_TTL = getattr(settings, "BARN_TASK_TTL", 30)  # days

    @classproperty
    def BARN_TASK_EAGER(cls):
        return getattr(settings, "BARN_TASK_EAGER", False)

    @classproperty
    def TASK_TTL(cls):
        return getattr(settings, "BARN_TASK_TTL", 30)

