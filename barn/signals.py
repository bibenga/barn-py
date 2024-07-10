from django.dispatch import Signal

# schedule
pre_schedule_execute = Signal()
# schedule
post_schedule_execute = Signal()

# task
pre_task_execute = Signal()
# task, exc
post_task_execute = Signal()
