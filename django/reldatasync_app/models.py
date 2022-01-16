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
    _seq = models.IntegerField()
    _deleted = models.BooleanField()

    @staticmethod
    def get_datastore_by_name(datastore_name, db_table, conn=None):
        if not conn:
            conn = connections['default']

        # get id for name if it exists
        ds_id = None
        try:
            row = DataSyncRevisions.objects.get(datastore_name=datastore_name)
            ds_id = row.datastore_id
        except DataSyncRevisions.DoesNotExist:
            # that's okay
            pass

        return PostgresDatastore(
            datastore_name,
            conn,
            db_table,
            datastore_id=ds_id)

    @classmethod
    def _get_datastore(cls, conn=None):
        return SyncableModel.get_datastore_by_name(
            cls.DatastoreMeta.datastore_name,
            cls._meta.db_table,
            conn)

    def assign_rev_and_seq(self):
        """Assign self._rev and self._seq with appropriate values"""
        with self._get_datastore() as pd:
            self._rev, self._seq = pd.new_rev_and_seq(self._rev)

    def save(self, *args, **kwargs):
        # Set _REV, _SEQ, _DELETED properly
        self.assign_rev_and_seq()
        self._deleted = False
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """Instead of removing the row, update it with _deleted True"""
        with self._get_datastore() as pd:
            # Set _REV, _SEQ, _DELETED properly
            self._rev, self._seq = pd.new_rev_and_seq(self._rev)
            self._deleted = True
        # Don't call super().delete(), since we want to keep the row

    class Meta:
        # See https://docs.djangoproject.com/en/4.0/topics/db/models/#abstract-base-classes  # noqa
        abstract = True

    class DatastoreMeta:
        datastore_name = None
