import logging
import psycopg2
import unittest
import random

from datastore import MemoryDatastore, PostgresDatastore, Document
from sync import sync_both

logger = logging.getLogger(__name__)


# nosetests ignores tests starting with underscore
# See also https://stackoverflow.com/a/50380006/34935
class _TestDatastore(unittest.TestCase):
    """Base class for testing datastores."""

    def setUp(self):
        self.server = None
        self.client = None

    def test_sync1(self):
        """Non-overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(Document({'_id': 'A', 'value': 'val1'}))
        # client makes object B v1
        self.client.put(Document({'_id': 'B', 'value': 'val2'}))

        # sync leaves both server and client with A val1, B val2
        sync_both(self.client, self.server)

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         self.client.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         self.client.get('B'))

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         self.server.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         self.server.get('B'))

    def test_overlapping_sync(self):
        """Overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(Document({'_id': 'A', 'value': 'val1'}))
        self.server.put(Document({'_id': 'C', 'value': 'val3'}))
        # client makes object B v1
        self.client.put(Document({'_id': 'B', 'value': 'val2'}))
        self.client.put(Document({'_id': 'C', 'value': 'val4'}))

        # sync leaves both server and client with A val1,  B val2, C val4
        sync_both(self.client, self.server)

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         self.client.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         self.client.get('B'))
        self.assertEqual(Document({'_id': 'C', 'value': 'val4', '_rev': 2}),
                         self.client.get('C'))

        self.assertEqual(Document({'_id': 'A', 'value': 'val1', '_rev': 1}),
                         self.server.get('A'))
        self.assertEqual(Document({'_id': 'B', 'value': 'val2', '_rev': 1}),
                         self.server.get('B'))
        self.assertEqual(Document({'_id': 'C', 'value': 'val4', '_rev': 2}),
                         self.server.get('C'))

    def test_long_streaks(self):
        items = ['a', 'b', 'c', 'd', 'e']

        for jdx in range(3):
            # some puts and deletes for server, then client
            num_steps = random.randint(2, 20)
            for idx in range(num_steps):
                # pick item
                item = random.choice(items)
                if random.uniform(0, 1) < 0.3:
                    self.server.delete(item)
                else:
                    val = random.randint(0, 1000)
                    self.server.put(Document({'_id': item, 'value': val}))

            num_steps = random.randint(2, 20)
            for idx in range(num_steps):
                item = random.choice(items)
                if random.uniform(0, 1) < 0.3:
                    self.client.delete(item)
                else:
                    val = random.randint(0, 1000)
                    self.client.put(Document({'_id': item, 'value': val}))

            # sync
            sync_both(self.client, self.server)

            # server and client should now contain the same stuff
            docs_c = [doc for doc in self.client.get_docs_since(0)]
            docs_s = [doc for doc in self.server.get_docs_since(0)]

            self.assertEqual(sorted(docs_c), sorted(docs_s))
        # self.fail("TEST")

    def test_copy(self):
        doc = Document({'_id': 'A', 'value': 'val1'})
        self.server.put(doc)
        doc['another'] = 'foo'
        doc2 = self.server.get('A')
        self.assertTrue('another' not in doc2)
        self.assertTrue('another' in doc)

    def test_delete(self):
        doc = Document({'_id': 'A', 'value': 'val1'})
        self.server.put(doc)
        doc1 = self.server.get('A')
        self.assertTrue(self.server.get('A'))
        self.server.delete('A')
        doc2 = self.server.get('A')
        import logging
        logging.info("doc2 %s" % doc2)
        self.assertEqual(True, doc2['_deleted'])
        self.assertGreater(doc2['_rev'], doc1['_rev'])


class TestMemoryDatastore(_TestDatastore):
    def setUp(self):
        super().setUp()
        self.server = MemoryDatastore('server')
        self.client = MemoryDatastore('client')


class TestPostgresDatastore(_TestDatastore):
    client_connstr = None
    server_connstr = None

    @staticmethod
    def _dbconnstr(dbname=None):
        """Connect to test database on host db, user postgres."""
        the_str = "host=db user=postgres"
        if dbname:
            the_str += ' dbname=%s' % dbname
        return the_str

    @staticmethod
    def exec_sql(exec_func, dbname=None, autocommit=True):
        """Execute some SQL.

        Sometimes we want it with dbname, sometimes without.
        Otherwise we'd just keep a cursor around instead.
        """
        connstr = TestPostgresDatastore._dbconnstr(dbname)
        with psycopg2.connect(connstr) as conn:
            if autocommit:
                # Autocommit to be able to create a database
                conn.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                exec_func(cursor)

    @staticmethod
    def _create_test_db(dbname):
        TestPostgresDatastore._create_test_db1(dbname)
        TestPostgresDatastore._create_test_db2(dbname)

    @staticmethod
    def _create_test_db1(dbname):
        def exec_func(curs):
            curs.execute(
                "SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
                (dbname,))
            if not curs.fetchone():
                curs.execute("CREATE DATABASE %s" % dbname)
            else:
                logger.info("database %s already exists" % dbname)
        TestPostgresDatastore.exec_sql(exec_func, autocommit=True)

    @staticmethod
    def _create_table_if_not_exists(dbname: str, tablename: str,
                                    definition: str):
        def exec_func(curs):
            # check for table existence
            curs.execute(
                "select * from information_schema.tables where table_name=%s",
                (tablename,))
            if not bool(curs.fetchone()):
                # table doesn't exist, create it
                curs.execute("CREATE TABLE %s (%s)" % (tablename, definition))
        TestPostgresDatastore.exec_sql(exec_func, dbname=dbname)

    @staticmethod
    def _create_test_db2(dbname):
        def exec_func(curs):
            TestPostgresDatastore._create_table_if_not_exists(
                dbname,
                'data_sync_revisions',
                'sequence_id int not null')
            TestPostgresDatastore._create_table_if_not_exists(
                dbname,
                'docs1',
                '_id text UNIQUE not null, _rev int not null,'
                ' value text, _deleted bool')
            TestPostgresDatastore._create_table_if_not_exists(
                dbname,
                'docs2',
                '_id text UNIQUE not null, _rev int not null,'
                ' value text, _deleted bool')
            # Init sequence_id if not present
            # "ON CONFLICT" requires postgres 9.5+
            # See also https://stackoverflow.com/a/17267423/34935
            # See also https://stackoverflow.com/a/30118648/34935
            curs.execute(
                "INSERT INTO data_sync_revisions (sequence_id) VALUES (0)"
                " ON CONFLICT DO NOTHING")
        TestPostgresDatastore.exec_sql(exec_func, dbname=dbname)

    @classmethod
    def setUpClass(cls):
        super(TestPostgresDatastore, cls).setUpClass()
        # create server and client databases
        cls.server_connstr = TestPostgresDatastore._dbconnstr('test_server')
        TestPostgresDatastore._create_test_db('test_server')
        cls.client_connstr = TestPostgresDatastore._dbconnstr('test_client')
        TestPostgresDatastore._create_test_db('test_client')

    @staticmethod
    def _drop_db(dbname):
        TestPostgresDatastore.exec_sql(
            lambda curs: curs.execute("DROP DATABASE %s" % dbname))

    @classmethod
    def tearDownClass(cls):
        TestPostgresDatastore._drop_db('test_server')
        TestPostgresDatastore._drop_db('test_client')

    def setUp(self):
        super().setUp()
        self.server = PostgresDatastore(
            'server', TestPostgresDatastore.server_connstr, 'docs1')
        self.server.__enter__()
        self.client = PostgresDatastore(
            'server', TestPostgresDatastore.client_connstr, 'docs2')
        self.client.__enter__()

    def tearDown(self) -> None:
        self.server.__exit__()
        self.client.__exit__()


class TestDocument(unittest.TestCase):
    def test_compare(self):
        doc = Document({'_id': 'A', 'value': 'val1'})
        self.assertEqual(doc, doc)
        doc2 = Document({'_id': 'A', 'value': 'val2'})
        self.assertGreater(doc2, doc)
        self.assertLess(doc, doc2)

    def test_none(self):
        doc1 = Document({'_id': 'A', 'value': 'val1'})
        doc2 = Document({'_id': 'A', 'value': None})
        # equality with None
        self.assertEqual(doc2, doc2)
        # inequality with None
        self.assertGreater(doc1, doc2)
        self.assertLess(doc2, doc1)
