import json
from datetime import datetime
from unittest import skip

from django.test import TestCase
from reldatasync import util
from reldatasync.datastore import MemoryDatastore
from reldatasync.replicator import Replicator
from reldatasync.vectorclock import VectorClock

from test_rsdb_app.models import Patient


class PatientTest(TestCase):
    def _create_patient(self):
        name = 'Yoinks'
        residence = 'Yoinkers'
        age = 30
        birth_date = datetime.strptime('2021-01-03', '%Y-%m-%d').date()
        self.patient = Patient(name=name, residence=residence, age=age,
                               birth_date=birth_date)
        self.patient.save()

    def test_create_delete(self):
        name = 'Yoinks'
        residence = 'Yoinkers'
        age = 30
        birth_date = datetime.strptime('2021-01-03', '%Y-%m-%d').date()
        pat = Patient(name=name, residence=residence, age=age,
                      birth_date=birth_date)
        pat.save()

        # Regular vars are still set
        self.assertEqual(name, pat.name)
        self.assertEqual(residence, pat.residence)
        self.assertEqual(age, pat.age)
        self.assertTrue(pat.created_dt)
        self.assertEqual(birth_date, pat.birth_date)

        # Syncable model vars are set
        self.assertEqual(1, pat._seq)
        self.assertTrue(pat._id)
        self.assertTrue(pat._rev)
        self.assertGreater(VectorClock.from_string(pat._rev), VectorClock({}))
        self.assertFalse(pat._deleted)

        pat2 = Patient.objects.get(name=name)
        self.assertEqual(pat, pat2)

        # Update pat, seq and rev go up
        rev = pat._rev
        pat.save()
        self.assertEqual(2, pat._seq)
        self.assertGreater(
            VectorClock.from_string(pat._rev), VectorClock.from_string(rev))

        # Delete pat, seq and rev go up, and row is still there _deleted True
        rev = pat._rev
        pat.delete()
        self.assertEqual(3, pat._seq)
        self.assertTrue(pat._deleted)
        self.assertGreater(
            VectorClock.from_string(pat._rev), VectorClock.from_string(rev))

    def test_get_put(self):
        # In with Django
        self._create_patient()
        pat = self.patient

        with Patient._get_datastore() as ds:
            # Out with Datastore.get
            pat2 = ds.get(pat._id)

            # Check datastore vars of pat2
            self.assertEqual(pat._seq, pat2['_seq'])
            self.assertTrue(pat._id, pat2['_id'])
            self.assertTrue(pat._rev, pat2['_rev'])
            self.assertFalse(pat2['_deleted'])

            # Make a new patient with the same attributes
            id_str = util.uuid4_string()
            pat2['_id'] = id_str

            # In with Datastore.put
            ds.put(pat2, increment_rev=True)

            # Out with Django
            pat3 = Patient.objects.get(_id=id_str)

            # pat3 datastore fields are all proper
            self.assertEqual(id_str, pat3._id)
            self.assertGreater(pat3._seq, pat2['_seq'])
            self.assertGreater(VectorClock.from_string(pat3._rev),
                               VectorClock.from_string(pat2['_rev']))
            self.assertFalse(pat3._deleted)

            # pat3 other fields are all proper
            self.assertEqual(pat.name, pat3.name)
            self.assertEqual(pat.residence, pat3.residence)
            self.assertEqual(pat.age, pat3.age)
            self.assertEqual(pat.created_dt, pat3.created_dt)
            self.assertEqual(pat.birth_date, pat3.birth_date)

    def test_sync(self):
        self._create_patient()
        pat = self.patient

        dsm = MemoryDatastore('test')
        with Patient._get_datastore() as ds:
            Replicator(ds, dsm).sync_both_directions()

            pat2 = dsm.get(pat._id)

            # check fields
            # pat3 datastore fields are all proper
            for field in (
                    '_id', '_seq', '_rev', '_deleted',
                    'name', 'residence', 'age', 'created_dt', 'birth_date'):
                self.assertEqual(getattr(pat, field), pat2[field])

    @skip
    def test_json_dumps(self):
        self._create_patient()

        with Patient._get_datastore() as ds:
            pat = ds.get(self.patient._id)
            self.assertTrue(json.dumps(pat))
