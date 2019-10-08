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
        self.assertTrue(('val1', 1), client.get('A'))
