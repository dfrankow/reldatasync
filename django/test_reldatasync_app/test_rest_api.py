import json
from datetime import datetime

from django.test import TestCase
from django.urls import reverse
# from reldatasync.document import Document, _ID, _DELETED

from rest_framework.test import APIClient

# from reldatasync_app import rest_api
from test_reldatasync_app.models import Organization, Patient


class RestApiTest(TestCase):
    def setUp(self) -> None:
        self.client = APIClient()

    def add_patient(self):
        # Add an org
        org = Organization(name='org1')
        org.save()

        # Add a Patient
        pat = Patient(name='name',
                      residence='residence',
                      age=30,
                      birth_date=datetime(2010, 1, 27).date(),
                      created_dt=datetime.now(),
                      email='name@example.com',
                      org=org)
        pat.save()

    def test_hello(self):
        resp = self.client.get(reverse('datastores'))
        self.assertEqual(200, resp.status_code)
        self.assertEqual({'datastores': ['organization', 'patient']},
                         json.loads(resp.content.decode('utf8')))

    def test_datastore_func(self):
        resp = self.client.get(reverse('datastore', args=['oops']))
        self.assertEqual(404, resp.status_code)

        resp = self.client.get(reverse('datastore', args=['patient']))
        self.assertEqual(200, resp.status_code)
        self.assertEqual(b'', resp.content)

    def test_sequence_id(self):
        resp = self.client.get(reverse(
            'sequence_id', args=['oops', 'source']))
        self.assertEqual(404, resp.status_code)

        resp = self.client.get(reverse(
            'sequence_id', args=['patient', 'source']))
        self.assertEqual(200, resp.status_code)
        self.assertEqual({'sequence_id': 0},
                         json.loads(resp.content.decode('utf8')))

    # TODO: test sequence_id_with_id

    def test_docs(self):
        resp = self.client.get(reverse('docs', args=['patient']))
        self.assertEqual(200, resp.status_code)
        self.assertEqual({'current_sequence_id': 0, 'documents': []},
                         json.loads(resp.content.decode('utf8')))

        # TODO: test docs POST
        # TODO: test with datastore actually populated

    # TODO: test doc
    # TODO: test doc_with_id
    # TODO: test sync?
