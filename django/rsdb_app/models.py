from django.db import models, connections

from reldatasync.datastore import PostgresDatastore
from reldatasync.util import uuid4_string


class DataSyncRevisions(models.Model):
    """Table needed by PostgresDatastore"""
    # NOTE: default is set by python library during init
    datastore_id = models.CharField(
        unique=True, primary_key=True, max_length=100)
    datastore_name = models.CharField(max_length=100, unique=True)
    sequence_id = models.IntegerField()

    class Meta:
        db_table = 'data_sync_revisions'


class SyncableModel(models.Model):
    # fields needed for PostgresDatastore: _id, _rev, _deleted
    _id = models.CharField(
        unique=True, primary_key=True, max_length=100,
        default=uuid4_string)
    _rev = models.CharField(max_length=2000)
    _deleted = models.BooleanField()

    @classmethod
    def _get_datastore(cls, conn):
        ds_name = cls.DatastoreMeta.datastore_name
        # get id for name if it exists
        ds_id = None
        try:
            row = DataSyncRevisions.objects.get(datastore_name=ds_name)
            ds_id = row.datastore_id
        except DataSyncRevisions.DoesNotExist:
            # that's okay
            pass

        return PostgresDatastore(
            ds_name,
            conn,
            cls._meta.db_table,
            datastore_id=ds_id)

    def save(self, *args, **kwargs):
        conn = connections['default']
        with self._get_datastore(conn) as pd:
            # Set _REV, _SEQ, _DELETED properly
            self._rev, self._seq = pd.new_rev_and_seq(self._rev)
            self._deleted = False
        super().save(*args, **kwargs)

    class Meta:
        # See https://docs.djangoproject.com/en/4.0/topics/db/models/#abstract-base-classes  # noqa
        abstract = True

    class DatastoreMeta:
        datastore_name = None
