# Generated by Django 5.0.6 on 2024-06-28 19:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("lock", "0004_alter_task_args_alter_task_error_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="schedule",
            old_name="payload",
            new_name="args",
        ),
        migrations.AddField(
            model_name="schedule",
            name="func",
            field=models.TextField(default="aaaa"),
            preserve_default=False,
        ),
    ]
