from django.test import TestCase
from reldatasync.vectorclock import VectorClock

from test_rsdb_app.models import Patient


class PatientTest(TestCase):
    def test_create(self):
        name = 'Yoinks'
        pat = Patient(name=name, residence='Yoinkers', age=30)
        pat.save()

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
