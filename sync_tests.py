import unittest

from .datastore import MemoryDatastore
from .sync import sync_both


class TestUtilFunctions(unittest.TestCase):
    def test_sync1(self):
        """Non-overlapping objects from datastore"""
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        # server makes object A v1
        server.put('A', 'val1')
        # client makes object B v1
        client.put('B', 'val2')

        # sync leaves both server and client with A v1 and B v1
        sync_both(client, server)

        self.assertEqual(('val1', 1), client.get('A'))
        self.assertEqual(('val2', 1), client.get('B'))

        self.assertEqual(('val1', 1), server.get('A'))
        self.assertEqual(('val2', 1), server.get('B'))

    def test_overlapping_sync(self):
        """Overlapping objects from datastore"""
        server = MemoryDatastore('server')
        client = MemoryDatastore('client')

        # server makes object A v1
        server.put('A', 'val1')
        server.put('C', 'val3')
        # client makes object B v1
        client.put('B', 'val2')
        client.put('C', 'val4')

        # sync leaves both server and client with A v1 and B v1
        sync_both(client, server)

        self.assertEqual(('val1', 1), client.get('A'))
        self.assertEqual(('val2', 1), client.get('B'))
        self.assertEqual(('val4', 2), client.get('C'))

        self.assertEqual(('val1', 1), server.get('A'))
        self.assertEqual(('val2', 1), server.get('B'))
        self.assertEqual(('val4', 2), client.get('C'))
