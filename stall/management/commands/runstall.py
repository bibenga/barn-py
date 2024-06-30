from barn.management.commands.runworker import Command as WorkerCommand


class Command(WorkerCommand):
    help = "Stall"
    scheduler_model = "stall.schedule1"
    task_model = "stall.task1"
