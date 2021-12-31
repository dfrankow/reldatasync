import json
from datetime import datetime

from django.test import TestCase
from reldatasync import util
from reldatasync.datastore import MemoryDatastore
from reldatasync.json import JsonEncoder, JsonDecoder
from reldatasync.replicator import Replicator
from reldatasync.vectorclock import VectorClock

from test_reldatasync_app.models import Patient, Organization


class PatientTest(TestCase):
    def _create_org(self):
        self.org = Organization(name='org')
        self.org.save()

    def _create_patient(self):
        self._create_org()
        self.name = 'Yoinks'
        self.residence = 'Yoinkers'
        self.age = 30
        self.birth_date = datetime.strptime('2021-01-03', '%Y-%m-%d').date()
        # test that EmailField (derived from CharField) works
        self.email = 'yoinks@example.com'
        self.patient = Patient(
            name=self.name, residence=self.residence, age=self.age,
            birth_date=self.birth_date, email=self.email,
            org=self.org)
        self.patient.save()

    def test_create_delete(self):
        self._create_patient()
        pat = self.patient

        # Regular vars are still set
        self.assertEqual(self.name, pat.name)
        self.assertEqual(self.residence, pat.residence)
        self.assertEqual(self.age, pat.age)
        self.assertTrue(pat.created_dt)
        self.assertEqual(self.birth_date, pat.birth_date)
        self.assertEqual(self.email, pat.email)
        self.assertEqual(self.org, pat.org)

        # Syncable model vars are set
        self.assertEqual(2, pat._seq)
        self.assertTrue(pat._id)
        self.assertTrue(pat._rev)
        self.assertGreater(VectorClock.from_string(pat._rev), VectorClock({}))
        self.assertFalse(pat._deleted)

        pat2 = Patient.objects.get(name=self.name)
        self.assertEqual(pat, pat2)

        # Update pat, seq and rev go up
        rev = pat._rev
        pat.save()
        self.assertEqual(3, pat._seq)
        self.assertGreater(
            VectorClock.from_string(pat._rev), VectorClock.from_string(rev))

        # Delete pat, seq and rev go up, and row is still there _deleted True
        rev = pat._rev
        pat.delete()
        self.assertEqual(4, pat._seq)
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

            # There are two patients with the same org, one from put()
            pats = [
                pat for pat in Patient.objects.filter(org__name=self.org.name)]
            self.assertEqual(2, len(pats))

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
            self.assertEqual(pat.email, pat3.email)
            self.assertEqual(pat.org, pat3.org)

    def test_sync(self):
        self._create_patient()
        pat = self.patient

        dsm = MemoryDatastore('test')

        # Could synchronize org with a different datastore
        # with Organization._get_datastore() as ds:
        #     Replicator(ds, org_dsm).sync_both_directions()

        with Patient._get_datastore() as ds:
            Replicator(ds, dsm).sync_both_directions()

            pat2 = dsm.get(pat._id)

            # check fields
            # pat3 datastore fields are all proper
            for field in (
                    '_id',
                    # _seq is different: dsm has only patients, while Patient's
                    # datastore has orgs and patients (in different tables).
                    # _seq can be different for different datastores, it's local
                    # '_seq',
                    '_rev', '_deleted',
                    'name', 'residence', 'age', 'created_dt', 'birth_date',
                    'org_id'):
                self.assertEqual(getattr(pat, field), pat2[field])

    def test_json_dumps(self):
        self._create_patient()

        with Patient._get_datastore() as ds:
            spat = self.patient
            pat = ds.get(spat._id)

            # encoding with datetime works
            pat_str = JsonEncoder().encode(pat)
            self.maxDiff = 2000
            self.assertEqual(
                f'{{"_id": "{spat._id}", '
                f'"_rev": {json.dumps(spat._rev)}, '
                '"_seq": 2, '
                '"_deleted": false, '
                '"name": "Yoinks", '
                '"residence": "Yoinkers", '
                '"age": 30, '
                '"birth_date": "2021-01-03", '
                f'"created_dt": "{spat.created_dt.isoformat()}", '
                '"email": "yoinks@example.com", '
                f'"org_id": "{self.org._id}"}}',
                pat_str)

            # The datetime has a format like this, ending in +00:00
            # 2021-12-30T17:07:27.918653+00:00
            self.assertIn('+00:00', pat_str)

            # decoding without schema does not work
            pat2 = JsonDecoder().decode(pat_str)
            with self.assertRaises(TypeError):
                self.assertEqual(pat, pat2)

            # decoding with schema works
            schema = {
                '_id': 'TEXT',
                '_seq': 'INTEGER',
                '_rev': 'TEXT',
                '_deleted': 'BOOLEAN',
                'name': 'TEXT',
                'residence': 'TEXT',
                'age': 'INTEGER',
                'birth_date': 'DATE',
                'created_dt': 'DATETIME',
                'email': 'TEXT',
                'org_id': 'TEXT',
            }
            pat2 = JsonDecoder(schema=schema).decode(pat_str)
            self.assertEqual(pat, pat2)
