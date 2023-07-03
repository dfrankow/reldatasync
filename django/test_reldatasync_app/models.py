from django.db import models
from django.utils import timezone

from reldatasync_app import rest_api
from reldatasync_app.models import SyncableModel


class Organization(SyncableModel):
    name = models.CharField(max_length=255, unique=True)

    class DatastoreMeta:
        datastore_name = 'reldatasync_project_server'


class Patient(SyncableModel):
    name = models.CharField(max_length=255)
    residence = models.CharField(max_length=255)
    age = models.IntegerField()
    birth_date = models.DateField()
    created_dt = models.DateTimeField(default=timezone.now)
    email = models.EmailField()
    org = models.ForeignKey(
        Organization,
        # Can't use CASCADE, because that wouldn't properly delete
        on_delete=models.PROTECT)

    class DatastoreMeta:
        datastore_name = 'reldatasync_project_server'


# Register datastores with the API
rest_api.add_datastore_class('organization', Organization)
rest_api.add_datastore_class('patient', Patient)
