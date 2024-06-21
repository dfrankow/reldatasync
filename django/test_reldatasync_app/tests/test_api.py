import json

from django.test import Client, TestCase
from django.urls import reverse
from test_reldatasync_app.models import DATASTORE_NAME, Organization


class ApiTest(TestCase):
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

    def test_get_doc(self):
        org = Organization(name="org")
        org.save()
        org2 = Organization(name="org2")
        org2.save()

        client = Client()
        the_url = reverse(
            "api-1.0.0:get_doc", args=[DATASTORE_NAME, "Organization", org2._id]
        )
        response = client.get(the_url)
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content)
        # _id, _rev are in there, but could be anything
        self.assertIn("_id", data)
        self.assertIn("_rev", data)
        self.assertIn("_seq", data)
        self.assertEqual("org2", data["name"])

        # Unknown doc
        the_url = reverse(
            "api-1.0.0:get_doc", args=[DATASTORE_NAME, "Organization", "oops"]
        )
        response = client.get(the_url)
        self.assertEqual(403, response.status_code, response.content)
        self.assertEqual(
            '{"message": "Doc \'oops\' not found"}', response.content.decode("utf-8")
        )

        # Unknown table
        the_url = reverse("api-1.0.0:get_doc", args=[DATASTORE_NAME, "oops", "Oops"])
        response = client.get(the_url)
        self.assertEqual(403, response.status_code, response.content)
        self.assertEqual(
            '{"message": "Unknown table \'oops\'"}', response.content.decode("utf-8")
        )
