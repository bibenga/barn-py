# barn-py

Lightweight scheduler and worker for Django using a database as backend.

The task worker and scheduler use the database as a queue to store and find tasks to process
and you can forget about using `transaction.on_commit`.

**If you need a billion messages per second, please leave.**

### Examples

#### Use generic models

```python
from django.http import HttpRequest, HttpResponse
from django.db import transaction
from barn.decorators import task

@transaction.atomic
def index(request: HttpRequest) -> HttpResponse:
    html = "<html><body>Hello</body></html>"
    # do something with database
    dummy.delay(view="index", code="12")
    return HttpResponse(html)


@task
def dummy(view: str, code: str) -> str:
    # task called inside transaction
    return "ok"
```

#### Use your models (prefered)

You can specify your own model for schedule and task and this approach is preferred.

```python
from django.db import models
from django.core.mail import send_mail
from barn.models import AbstractTask


class EndOfTrialPeriodTask(AbstractTask):
    user = models.ForeignKey('auth.User', models.CASCADE)

    def process(self) -> None:
        # go somewhere and do something...
        self.user.is_active = False
        self.user.save()
        NotifyEndOfTrialPeriodTask.objects.create(user_id=self.user_id)


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


class EndOfSubscriptionTask(AbstractTask):
    user = models.ForeignKey('auth.User', models.CASCADE)

    def process(self) -> None:
        # go somewhere and do something...
```
