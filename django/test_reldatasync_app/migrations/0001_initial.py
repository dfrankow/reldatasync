# Generated by Django 3.2.10 on 2021-12-30 21:45

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import reldatasync.util


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Organization',
            fields=[
                ('_id', models.CharField(default=reldatasync.util.uuid4_string, max_length=100, primary_key=True, serialize=False, unique=True)),
                ('_rev', models.CharField(max_length=2000)),
                ('_seq', models.IntegerField()),
                ('_deleted', models.BooleanField()),
                ('name', models.CharField(max_length=255)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Patient',
            fields=[
                ('_id', models.CharField(default=reldatasync.util.uuid4_string, max_length=100, primary_key=True, serialize=False, unique=True)),
                ('_rev', models.CharField(max_length=2000)),
                ('_seq', models.IntegerField()),
                ('_deleted', models.BooleanField()),
                ('name', models.CharField(max_length=255)),
                ('residence', models.CharField(max_length=255)),
                ('age', models.IntegerField()),
                ('birth_date', models.DateField()),
                ('created_dt', models.DateTimeField(default=django.utils.timezone.now)),
                ('email', models.EmailField(max_length=254)),
                ('org', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='test_reldatasync_app.organization')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]