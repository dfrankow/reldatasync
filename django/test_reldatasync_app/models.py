from django.db import models
from django.utils import timezone
from reldatasync_app.models import SyncableModel

DATASTORE_NAME = "reldatasync_project_datastore"


class Organization(SyncableModel):
    name = models.CharField(max_length=255, unique=True)

    class DatastoreMeta:
        datastore_name = DATASTORE_NAME


class IntermediateSyncableModel(SyncableModel):
    """This is an intermediate model to test SyncableModel.get_table_by_class_name"""

    class Meta:
        # See https://docs.djangoproject.com/en/5.0/topics/db/models/#abstract-base-classes  # noqa: E501
        abstract = True

    class DatastoreMeta:
        datastore_name = DATASTORE_NAME


class Patient(IntermediateSyncableModel):
    name = models.CharField(max_length=255)
    residence = models.CharField(max_length=255)
    age = models.IntegerField()
    birth_date = models.DateField()
    created_dt = models.DateTimeField(default=timezone.now)
    email = models.EmailField()
    org = models.ForeignKey(
        Organization,
        # Can't use CASCADE, because that wouldn't properly delete
        on_delete=models.PROTECT,
    )

    class DatastoreMeta:
        datastore_name = DATASTORE_NAME
