from django.test import TransactionTestCase
from reldatasync.datastore import NoSuchTable
from reldatasync_app.models import DataSyncRevisions, SyncableModel
from test_reldatasync_app.models import DATASTORE_NAME, Organization, Patient


class ModelsTest(TransactionTestCase):
    def test_syncable_model_get_table(self):
        # direct descendant
        self.assertEqual(
            Organization._meta.db_table,
            SyncableModel.get_table_by_class_name("Organization"),
        )
        # indirect descendant
        self.assertEqual(
            Patient._meta.db_table, SyncableModel.get_table_by_class_name("Patient")
        )
        self.assertIsNone(SyncableModel.get_table_by_class_name("oops"))

    def test_syncable_model_get_datastore(self):
        # bad names still result in a datastore object, because it is a wrapper
        # It won't work, though
        self.assertEqual(0, DataSyncRevisions.objects.count())
        ds = SyncableModel.get_datastore_by_name("what", "foo")
        with self.assertRaises(NoSuchTable):
            # pylint: disable-next=unnecessary-dunder-call
            ds.__enter__()
        # asking for a datastore conjured it
        self.assertEqual(1, DataSyncRevisions.objects.count())
        # In this test, we roll back.
        # ds.conn.rollback()
        ds.__exit__()

        # Okay names, but nothing in the Organization table yet
        table = SyncableModel.get_table_by_class_name("Organization")
        ds = SyncableModel.get_datastore_by_name(DATASTORE_NAME, table)
        # enter and exit without error, but no objects
        with ds:
            self.assertEqual(0, ds.sequence_id)

        org = Organization(name="org")
        org.save()
        with ds:
            # now we have an org
            self.assertEqual(1, ds.sequence_id)
