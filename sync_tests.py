import logging
import psycopg2
import unittest
import random

from datastore import MemoryDatastore, PostgresDatastore, Document, _ID, _REV

logger = logging.getLogger(__name__)


# nosetests ignores tests starting with underscore
# See also https://stackoverflow.com/a/50380006/34935
class _TestDatastore(unittest.TestCase):
    """Base class for testing datastores."""

    def setUp(self):
        self.server = None
        self.client = None

    def test_nonoverlapping(self):
        """Non-overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(Document({_ID: 'A', 'value': 'val1'}))
        # client makes object B v1
        self.client.put(Document({_ID: 'B', 'value': 'val2'}))

        # sync leaves both server and client with A val1, B val2
        self.client.sync_both_directions(self.server)

        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
                         self.client.get('A'))
        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
                         self.client.get('B'))

        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
                         self.server.get('A'))
        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
                         self.server.get('B'))

        # counter is at the highest existing doc version
        server_seq, server_docs = self.server.get_docs_since(0, 1000)
        self.assertEqual(self.server.sequence_id, server_seq)
        self.assertEqual(self.server.sequence_id,
                         max(doc[_REV] for doc in server_docs))

        client_seq, client_docs = self.client.get_docs_since(0, 1000)
        self.assertEqual(self.client.sequence_id, client_seq)
        self.assertEqual(self.client.sequence_id,
                         max(doc[_REV] for doc in client_docs))

    def test_put_if_needed(self):
        """put_if_needed doesn't put a second time"""
        doc = Document({_ID: 'A', 'value': 'val1'})
        # put the doc
        self.assertEqual(1, self.server.put_if_needed(doc))
        # doc is already present
        self.assertEqual(0, self.server.put_if_needed(doc))

    def test_overlapping_sync(self):
        """Overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(Document({_ID: 'A', 'value': 'val1'}))
        self.server.put(Document({_ID: 'C', 'value': 'val3'}))
        # client makes object B v1
        self.client.put(Document({_ID: 'B', 'value': 'val2'}))
        self.client.put(Document({_ID: 'C', 'value': 'val4'}))

        # sync leaves both server and client with A val1,  B val2, C val4
        self.client.sync_both_directions(self.server)

        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
                         self.client.get('A'))
        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
                         self.client.get('B'))
        self.assertEqual(Document({_ID: 'C', 'value': 'val4', _REV: 2}),
                         self.client.get('C'))

        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
                         self.server.get('A'))
        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
                         self.server.get('B'))
        self.assertEqual(Document({_ID: 'C', 'value': 'val4', _REV: 2}),
                         self.server.get('C'))

    @staticmethod
    def _some_datastore_mods(datastore, items):
        num_steps = random.randint(2, 30)
        for idx in range(num_steps):
            # pick item
            item = random.choice(items)
            if random.uniform(0, 1) < 0.3:
                datastore.delete(item)
            else:
                val = random.randint(0, 1000)
                datastore.put(Document({_ID: item, 'value': val}))

    def test_long_streaks(self):
        items = ['a', 'b', 'c', 'd', 'e']

        for jdx in range(4):
            # some mods for server, then client
            _TestDatastore._some_datastore_mods(self.server, items)
            _TestDatastore._some_datastore_mods(self.client, items)

            # sync
            # use small chunk size to test multiple chunks
            self.client.sync_both_directions(self.server, chunk_size=2)

            # server and client should now contain the same stuff
            _, docs_c = self.client.get_docs_since(0, 1000)
            _, docs_s = self.server.get_docs_since(0, 1000)

            self.assertEqual(sorted(docs_c), sorted(docs_s))

    def test_copy(self):
        doc = Document({_ID: 'A', 'value': 'val1'})
        self.server.put(doc)
        doc['another'] = 'foo'
        doc2 = self.server.get('A')
        self.assertTrue('another' not in doc2)
        self.assertTrue('another' in doc)

    def test_delete(self):
        doc = Document({_ID: 'A', 'value': 'val1'})
        self.server.put(doc)
        doc1 = self.server.get('A')
        self.assertTrue(self.server.get('A'))
        self.server.delete('A')
        doc2 = self.server.get('A')
        import logging
        logging.info("doc2 %s" % doc2)
        self.assertEqual(True, doc2['_deleted'])
        self.assertGreater(doc2[_REV], doc1[_REV])


class TestMemoryDatastore(_TestDatastore):
    def setUp(self):
        super().setUp()
        self.server = MemoryDatastore('server')
        self.client = MemoryDatastore('client')


class TestPostgresDatastore(_TestDatastore):
    client_connstr = None
    server_connstr = None
    # SAME_DB: if True, put two tables in one DB; else put one table in two DBs.
    SAME_DB = True

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
        TestPostgresDatastore._create_test_tables(dbname)

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
    def _create_test_tables(dbname):
        def exec_func(curs):
            TestPostgresDatastore._create_table_if_not_exists(
                dbname,
                'data_sync_revisions',
                'datastore_id text not null, sequence_id int not null')
            # docs1 only needed on server, and docs2 on client
            # but it's easier to just create both tables on both
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
        TestPostgresDatastore.exec_sql(exec_func, dbname=dbname)

    @staticmethod
    def _clear_tables(dbname):
        def exec_func(curs):
            curs.execute("DELETE FROM data_sync_revisions")
            curs.execute("DELETE FROM docs1")
            curs.execute("DELETE FROM docs2")
        TestPostgresDatastore.exec_sql(exec_func, dbname=dbname)

    @classmethod
    def setUpClass(cls):
        super(TestPostgresDatastore, cls).setUpClass()
        # create server and client databases
        cls.server_dbname = 'test_server'
        cls.server_connstr = TestPostgresDatastore._dbconnstr(cls.server_dbname)
        TestPostgresDatastore._create_test_db(cls.server_dbname)

        if TestPostgresDatastore.SAME_DB:
            cls.client_dbname = cls.server_dbname
            cls.client_connstr = cls.server_connstr
        else:
            cls.client_dbname = 'test_client'
            cls.client_connstr = TestPostgresDatastore._dbconnstr(
                cls.client_dbname)
            TestPostgresDatastore._create_test_db(cls.client_dbname)

    @staticmethod
    def _drop_db(dbname):
        TestPostgresDatastore.exec_sql(
            lambda curs: curs.execute("DROP DATABASE %s" % dbname))

    @classmethod
    def tearDownClass(cls):
        TestPostgresDatastore._drop_db(cls.server_dbname)
        if not TestPostgresDatastore.SAME_DB:
            TestPostgresDatastore._drop_db(cls.client_dbname)

    def setUp(self):
        super().setUp()
        # Clear tables for server and client
        TestPostgresDatastore._clear_tables(
            TestPostgresDatastore.server_dbname)
        TestPostgresDatastore._clear_tables(
            TestPostgresDatastore.client_dbname)

        self.server = PostgresDatastore(
            'server', TestPostgresDatastore.server_connstr, 'docs1')
        self.server.__enter__()
        self.client = PostgresDatastore(
            'client', TestPostgresDatastore.client_connstr, 'docs2')
        self.client.__enter__()

    def tearDown(self) -> None:
        self.server.__exit__()
        self.client.__exit__()


class TestDocument(unittest.TestCase):
    def test_compare(self):
        doc = Document({_ID: 'A', 'value': 'val1'})
        self.assertEqual(doc, doc)
        doc2 = Document({_ID: 'A', 'value': 'val2'})
        self.assertGreater(doc2, doc)
        self.assertLess(doc, doc2)

    def test_none(self):
        doc1 = Document({_ID: 'A', 'value': 'val1'})
        doc2 = Document({_ID: 'A', 'value': None})
        # equality with None
        self.assertEqual(doc2, doc2)
        # inequality with None
        self.assertGreater(doc1, doc2)
        self.assertLess(doc2, doc1)
