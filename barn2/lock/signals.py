from django.dispatch import Signal

leader_changed = Signal()

schedule_execute = Signal()

task_pre_execute = Signal()
task_post_execute = Signal()
