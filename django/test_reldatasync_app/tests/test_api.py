import json

from django.test import Client, TransactionTestCase
from django.urls import reverse
from test_reldatasync_app.models import DATASTORE_NAME, Organization


class ApiTest(TransactionTestCase):
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
            '{"detail": "Doc \'oops\' not found"}', response.content.decode("utf-8")
        )

        # Unknown table
        the_url = reverse("api-1.0.0:get_doc", args=[DATASTORE_NAME, "oops", "Oops"])
        response = client.get(the_url)
        self.assertEqual(403, response.status_code, response.content)
        self.assertEqual(
            '{"detail": "Unknown table \'oops\'"}', response.content.decode("utf-8")
        )

    def test_put_doc(self):
        self.assertEqual(0, Organization.objects.count())
        client = Client()
        the_url = reverse("api-1.0.0:post_doc", args=[DATASTORE_NAME, "Organization"])

        # everything empty (no table) should error
        response = client.post(the_url, content_type="application/json")
        self.assertEqual(422, response.status_code, response.content)
        self.assertEqual(
            b'{"detail": "Document must have _id"}',
            response.content,
        )

        # create table
        org = Organization(name="name")
        org.save()

        # empty doc should error
        response = client.post(the_url, body="{}", content_type="application/json")
        self.assertEqual(422, response.status_code, response.content)
        self.assertEqual(
            b'{"detail": "Document must have _id"}',
            response.content,
        )

        the_id = "aaa"
        # no _id
        name = "org name"
        response = client.post(
            the_url,
            data={"name": name},
            content_type="application/json",
        )
        self.assertEqual(422, response.status_code, response.content)
        self.assertEqual(b'{"detail": "Document must have _id"}', response.content)
        # self.assertEqual("org2", data)

        # no _rev
        response = client.post(
            the_url,
            data={"_id": the_id, "name": name},
            content_type="application/json",
        )
        self.assertEqual(422, response.status_code, response.content)
        self.assertEqual(
            b'{"detail": "doc aaa must have _rev if increment_rev is False"}',
            response.content,
        )

        # valid
        with self.assertRaises(Organization.DoesNotExist):
            Organization.objects.get(name=name)

        response = client.post(
            the_url,
            data={"_id": the_id, "_rev": "{}", "name": name},
            content_type="application/json",
        )
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEqual(
            {
                "num_docs_put": 1,
                "document": {"_id": the_id, "_rev": "{}", "name": name, "_seq": 2},
            },
            data,
            data,
        )
        # And it actually got saved
        org = Organization.objects.get(name=name)
        self.assertEqual(the_id, org._id)
        self.assertEqual("{}", org._rev)
        self.assertEqual(2, org._seq)
