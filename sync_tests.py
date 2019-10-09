import unittest

from .datastore import MemoryDatastore, Document
from .sync import sync_both


class TestUtilFunctions(unittest.TestCase):
    def test_sync1(self):
        """Non-overlapping objects from datastore"""
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        # server makes object A v1
        server.put(Document({'_id': 'A', 'value': 'val1'}))
        # client makes object B v1
        client.put(Document({'_id': 'B', 'value': 'val2'}))

        # sync leaves both server and client with A v1 and B v1
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
        """Overlapping objects from datastore"""
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        # server makes object A v1
        server.put(Document({'_id': 'A', 'value': 'val1'}))
        server.put(Document({'_id': 'C', 'value': 'val3'}))
        # client makes object B v1
        client.put(Document({'_id': 'B', 'value': 'val2'}))
        client.put(Document({'_id': 'C', 'value': 'val4'}))

        # sync leaves both server and client with A v1 and B v1
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
