from django.db import models
from django.utils import timezone

from rsdb_app.models import SyncableModel


class Patient(SyncableModel):
    name = models.CharField(max_length=255)
    residence = models.CharField(max_length=255)
    age = models.IntegerField()
    birth_date = models.DateField()
    created_dt = models.DateTimeField(default=timezone.now)
    email = models.EmailField()

    class DatastoreMeta:
        datastore_name = 'rsdb_server_patient'
