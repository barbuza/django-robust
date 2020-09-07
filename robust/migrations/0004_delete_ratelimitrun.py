from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("robust", "0003_traceback"),
    ]

    operations = [
        migrations.DeleteModel(
            name="RateLimitRun",
        ),
    ]
