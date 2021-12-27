from django.db import models

from rsdb_app.models import SyncableModel


class Patient(SyncableModel):
    name = models.CharField(max_length=255)
    residence = models.CharField(max_length=255)
    age = models.IntegerField()

    class DatastoreMeta:
        datastore_name = 'rsdb_server_patient'
