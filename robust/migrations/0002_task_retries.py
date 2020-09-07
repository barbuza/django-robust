from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("robust", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="task",
            name="retries",
            field=models.IntegerField(null=True),
        ),
    ]
