import json

from django.test import Client, TestCase
from django.urls import reverse
from test_reldatasync_app.models import DATASTORE_NAME, Organization


class PatientTest(TestCase):
    def test_datastores(self):
        client = Client()
        response = client.get(reverse("api-1.0.0:datastores"))
        self.assertEqual(200, response.status_code)
        data = json.loads(response.content)
        self.assertEqual(
            [],
            data,
        )

        # Make a datastore
        org = Organization(name="org")
        org.save()

        # Now it's in there
        response = client.get(reverse("api-1.0.0:datastores"))
        self.assertEqual(200, response.status_code)
        data = json.loads(response.content)
        self.assertEqual(1, len(data))
        self.assertEqual(DATASTORE_NAME, data[0]["name"])
        self.assertIn("id", data[0])
