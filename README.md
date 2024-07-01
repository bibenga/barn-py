# barn-py

Lightweight scheduler and worker for Django using a database backend.

Schedules and tasks store in a database.

The scheduler uses a database to find schedules to process.
Schedules are processed in a transaction with `select ... for update`.

The task worker uses a database as a queue to find tasks to process.
Tasks are created in a database and you can forget about using `transaction.on_commit`.
Tasks are processed in a transaction with `select ... for update`.

You can use Barn's models or create your own.

If you need billion messages per seconds you should go away.

### Examples

#### Use Barn's models
Create a task in file tasks.py
```python
import logging
from barn.decorators import task

@task
def dummy(view: str, code: str) -> str:
    # task called inside transaction
    return "ok"
```

Call the task in some view
```python
from django.http import HttpRequest, HttpResponse
from django.db import transaction
from tasks import dummy

@transaction.atomic
def index(request: HttpRequest) -> HttpResponse:
    html = "<html><body>Hello</body></html>"
    dummy.delay(view="index", code="12")
    return HttpResponse(html)
```

#### Use your models
Create a task in file models.py
```python
from django.db import models
from django.core.mail import send_mail
from barn.models import AbstractSchedule, AbstractTask

class EndOfTrialPeriodSchedule(AbstractSchedule):
    user = models.ForeignKey('auth.User', models.CASCADE)

    def process(self) -> None:
        EndOfTrialPeriodTask.objects.create(user_id=self.user_id)
        NotifyEndOfTrialPeriodTask.objects.create(user_id=self.user_id)


class EndOfTrialPeriodTask(AbstractTask):
    user = models.ForeignKey('auth.User', models.CASCADE)

    def process(self) -> None:
        # go somewhere and do something...
        self.user.is_active = False
        self.user.save()


class NotifyEndOfTrialPeriodTask(AbstractTask):
    user = models.ForeignKey('auth.User', models.CASCADE)

    def process(self) -> None:
        if self.user.email:
            send_mail(
                "End of trial period",
                f"Hello {self.user.get_full_name()}. Your trial period has ended.",
                "from@example.com",
                [self.user.email],
                fail_silently=False,
            )
```
