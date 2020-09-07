from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("robust", "0002_task_retries"),
    ]

    operations = [
        migrations.AddField(
            model_name="task",
            name="traceback",
            field=models.TextField(null=True),
        ),
        migrations.AddField(
            model_name="taskevent",
            name="traceback",
            field=models.TextField(null=True),
        ),
    ]
