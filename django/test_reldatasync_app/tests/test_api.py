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

    def test_get_docs(self):
        client = Client()
        the_url = reverse("api-1.0.0:get_docs", args=[DATASTORE_NAME, "Organization"])

        # empty
        self.assertEqual(0, Organization.objects.count())
        response = client.get(the_url, data={"start_sequence_id": 0})
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEqual({"current_sequence_id": 0, "documents": []}, data)

        # Add two orgs
        name1 = "name1"
        org1 = Organization(name=name1)
        org1.save()
        name2 = "name2"
        org2 = Organization(name=name2)
        org2.save()

        # Get them both
        response = client.get(the_url, data={"start_sequence_id": 0})
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEqual(2, data["current_sequence_id"])
        self.assertEqual(2, len(data["documents"]))
        self.assertEqual(1, data["documents"][0]["_seq"])
        self.assertEqual(name1, data["documents"][0]["name"])
        self.assertEqual(2, data["documents"][1]["_seq"])
        self.assertEqual(name2, data["documents"][1]["name"])

        # Get only the second
        response = client.get(the_url, data={"start_sequence_id": 1})
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEqual(2, data["current_sequence_id"])
        self.assertEqual(1, len(data["documents"]))
        self.assertEqual(2, data["documents"][0]["_seq"])
        self.assertEqual(name2, data["documents"][0]["name"])

    def test_put_docs(self):
        client = Client()
        the_url = reverse("api-1.0.0:put_docs", args=[DATASTORE_NAME, "Organization"])

        # POST three docs
        self.assertEqual(0, Organization.objects.count())
        three_docs = [
            {"_id": f"id{the_id}", "_rev": "{}", "name": f"name{the_id}"}
            for the_id in range(3)
        ]
        response = client.post(
            the_url, data=three_docs, content_type="application/json"
        )
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEqual(3, data["num_docs_put"])
        self.assertEqual(3, len(data["documents"]))
        for the_id in range(3):
            self.assertEqual(f"id{the_id}", data["documents"][the_id]["_id"])
            self.assertEqual(f"name{the_id}", data["documents"][the_id]["name"])
            # TODO: return the updated revs?
            # rev = data["documents"][the_id]["_rev"]
            # self.assertTrue(len(rev) > 2, rev)
        # docs are in the DB
        self.assertEqual(3, Organization.objects.count())

        # POST again, nothing should change
        response = client.post(
            the_url, data=three_docs, content_type="application/json"
        )
        self.assertEqual(200, response.status_code, response.content)
        data = json.loads(response.content.decode("utf-8"))
        self.assertEqual(0, data["num_docs_put"])
        self.assertEqual(0, len(data["documents"]))
