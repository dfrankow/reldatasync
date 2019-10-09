import unittest
import random

from .datastore import MemoryDatastore, Document
from .sync import sync_both


class TestDatastore(unittest.TestCase):
    def test_sync1(self):
        """Non-overlapping documents from datastore"""
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        # server makes object A v1
        server.put(Document({'_id': 'A', 'value': 'val1'}))
        # client makes object B v1
        client.put(Document({'_id': 'B', 'value': 'val2'}))

        # sync leaves both server and client with A val1, B val2
        sync_both(client, server)

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         client.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         client.get('B'))

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         server.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         server.get('B'))

    def test_overlapping_sync(self):
        """Overlapping documents from datastore"""
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        # server makes object A v1
        server.put(Document({'_id': 'A', 'value': 'val1'}))
        server.put(Document({'_id': 'C', 'value': 'val3'}))
        # client makes object B v1
        client.put(Document({'_id': 'B', 'value': 'val2'}))
        client.put(Document({'_id': 'C', 'value': 'val4'}))

        # sync leaves both server and client with A val1,  B val2, C val4
        sync_both(client, server)

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         client.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         client.get('B'))
        self.assertEqual(Document({'_id': 'C', 'value': 'val4', '_rev': 2}),
                         client.get('C'))

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         server.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         server.get('B'))
        self.assertEqual(Document({'_id': 'C', 'value': 'val4', '_rev': 2}),
                         server.get('C'))

    def test_long_streaks(self):
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        items = ['a', 'b', 'c', 'd', 'e']

        for jdx in range(10):
            # 50 puts for server and client
            for idx in range(50):
                # pick item
                item = random.choice(items)
                val = random.randint(0, 1000)
                server.put(Document({'_id': item, 'value': val}))

            for idx in range(30):
                item = random.choice(items)
                val = random.randint(0, 1000)
                client.put(Document({'_id': item, 'value': val}))

            # sync
            sync_both(client, server)

            # server and client should now contain the same stuff
            docs_c = [doc for doc in client.get_docs_since(0)]
            docs_s = [doc for doc in server.get_docs_since(0)]

            self.assertEqual(sorted(docs_c), sorted(docs_s))

    def test_copy(self):
        server = MemoryDatastore('server')
        doc = Document({'_id': 'A', 'value': 'val1'})
        server.put(doc)
        doc['another'] = 'foo'
        doc2 = server.get('A')
        self.assertTrue('another' not in doc2)
        self.assertTrue('another' in doc)


class TestDocument(unittest.TestCase):
    def test_compare(self):
        doc = Document({'_id': 'A', 'value': 'val1'})
        self.assertEqual(doc, doc)
        doc2 = Document({'_id': 'A', 'value': 'val2'})
        self.assertGreater(doc2, doc)
        self.assertLess(doc, doc2)
