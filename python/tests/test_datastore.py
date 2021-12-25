import logging
import os
import random

import psycopg2
import unittest

from reldatasync.datastore import (
    MemoryDatastore, PostgresDatastore)
from reldatasync.document import Document, _REV, _ID

logger = logging.getLogger(__name__)

# Get log level from environment so we can set it for python -m unittest
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'WARNING'))


# Tests starting with underscore are ignored
# See also https://stackoverflow.com/a/50380006
class _TestDatastore:
    """Base class for testing datastores."""

    def setUp(self):
        self.server = None
        self.client = None
        self.third = None

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

    def test_three_servers(self):
        # If we have three servers A, B, C
        # and A syncs with B, B with C, but A never syncs with C
        # we should still have all three servers agree
        # server makes object A v1
        self.server.put(Document({_ID: 'A', 'value': 'val1'}))
        self.server.put(Document({_ID: 'D', 'value': 'val3'}))
        # client makes object B v1
        self.client.put(Document({_ID: 'B', 'value': 'val2'}))
        self.client.put(Document({_ID: 'D', 'value': 'val4'}))
        # third makes object C v1
        self.third.put(Document({_ID: 'C', 'value': 'val3'}))
        self.third.put(Document({_ID: 'D', 'value': 'val5'}))

        # pull server <= client
        logger.debug("*** pull server <= client")
        self.server.pull_changes(self.client)
        # pull client <= third
        logger.debug("*** pull client <= third")
        self.client.pull_changes(self.third)
        # pull server <= client
        logger.debug("*** pull server <= client")
        self.server.pull_changes(self.client)

        # third only has C and D, since nothing pushed to it
        self.assertEqual(Document({_ID: 'C', 'value': 'val3', _REV: 1}),
                         self.third.get('C'))
        self.assertEqual(Document({_ID: 'D', 'value': 'val5', _REV: 2}),
                         self.third.get('D'))

        # now server has all of third's docs even though they never synced,
        # because server got third's changes through client
        self.assertEqual(self.server.sequence_id, self.third.sequence_id)

        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
                         self.server.get('A'))
        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
                         self.server.get('B'))

        self.assertEqual(Document({_ID: 'C', 'value': 'val3', _REV: 1}),
                         self.client.get('C'))
        # currently fails because sequence_id is used improperly to fetch
        # changes.  server never got client's C.
        self.assertEqual(Document({_ID: 'C', 'value': 'val3', _REV: 1}),
                         self.server.get('C'))

        self.assertEqual(Document({_ID: 'D', 'value': 'val5', _REV: 4}),
                         self.server.get('D'))

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
        self.assertTrue(doc1)
        self.server.delete('A')

        # get doesn't return deleted doc by default
        self.assertIsNone(self.server.get('A'))

        # get returns deleted doc if asked
        doc2 = self.server.get('A', include_deleted=True)
        self.assertEqual(True, doc2['_deleted'])
        self.assertGreater(doc2[_REV], doc1[_REV])


class TestMemoryDatastore(_TestDatastore, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.server = MemoryDatastore('server')
        self.client = MemoryDatastore('client')
        self.third = MemoryDatastore('third')


def _dbconnstr(dbname=None):
    """Connect to test database on host db, user postgres by default.

    You can change host and server with environment variables POSTGRES_HOST
    and POSTGRES_SERVER.
    """
    host = os.getenv('POSTGRES_HOST', 'localhost')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', None)
    the_str = f"host={host} user={user}"
    if dbname:
        the_str += f' dbname={dbname}'
    if password:
        the_str += f' password={password}'
    return the_str


def exec_sql(exec_func, dbname=None, autocommit=True):
    """Execute some SQL.

    Sometimes we want it with dbname, sometimes without.
    Otherwise we'd just keep a cursor around instead.
    """
    connstr = _dbconnstr(dbname)
    conn = None
    # NOTE: This particular style of executing a command is to allow
    # creating a database with later versions of psycopg2.
    # See https://stackoverflow.com/a/68112827
    try:
        conn = psycopg2.connect(connstr)
        if autocommit:
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cursor:
            exec_func(cursor)
    finally:
        if conn:
            conn.close()


def _create_test_db(dbname):
    _create_test_db1(dbname)
    _create_test_tables(dbname)


def _create_test_db1(dbname):
    def exec_func(curs):
        curs.execute(
            "SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
            (dbname,))
        if not curs.fetchone():
            curs.execute("CREATE DATABASE %s" % dbname)
        else:
            logger.info("database %s already exists" % dbname)
    exec_sql(exec_func, autocommit=True)


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
    exec_sql(exec_func, dbname=dbname)


def _create_test_tables(dbname):
    def exec_func(curs):
        _create_table_if_not_exists(
            dbname,
            'data_sync_revisions',
            'datastore_id text not null, sequence_id int not null')
        # docs1 only needed on server, and docs2 on client
        # but it's easier to just create both tables on both
        _create_table_if_not_exists(
            dbname,
            'docs1',
            '_id text UNIQUE not null, _rev int not null,'
            ' value text, _deleted bool')
        _create_table_if_not_exists(
            dbname,
            'docs2',
            '_id text UNIQUE not null, _rev int not null,'
            ' value text, _deleted bool')
    exec_sql(exec_func, dbname=dbname)


def _clear_tables(dbname):
    def exec_func(curs):
        curs.execute("DELETE FROM data_sync_revisions")
        curs.execute("DELETE FROM docs1")
        curs.execute("DELETE FROM docs2")
    exec_sql(exec_func, dbname=dbname)


def _drop_db(dbname):
    exec_sql(lambda curs: curs.execute("DROP DATABASE %s" % dbname))


def _create_databases(cls, same_db):
    # create server and client databases
    cls.server_dbname = 'test_server'
    cls.server_connstr = _dbconnstr(cls.server_dbname)
    _create_test_db(cls.server_dbname)

    if same_db:
        cls.client_dbname = cls.server_dbname
        cls.client_connstr = cls.server_connstr
    else:
        cls.client_dbname = 'test_client'
        cls.client_connstr = _dbconnstr(cls.client_dbname)
        _create_test_db(cls.client_dbname)

    cls.third_dbname = 'test_third'
    cls.third_connstr = _dbconnstr(cls.third_dbname)
    _create_test_db(cls.third_dbname)


def _drop_databases(cls, same_db):
    _drop_db(cls.server_dbname)
    if not same_db:
        _drop_db(cls.client_dbname)
    _drop_db(cls.third_dbname)


class TestPostgresDatastore(_TestDatastore, unittest.TestCase):
    client_connstr = None
    server_connstr = None
    third_connstr = None
    # SAME_DB: if True, put server/client tables in one DB;
    # else put one table in two DBs.
    SAME_DB = True

    @classmethod
    def setUpClass(cls):
        super(TestPostgresDatastore, cls).setUpClass()
        _create_databases(cls, TestPostgresDatastore.SAME_DB)

    @classmethod
    def tearDownClass(cls):
        super(TestPostgresDatastore, cls).tearDownClass()
        _drop_databases(cls, TestPostgresDatastore.SAME_DB)

    def setUp(self):
        super().setUp()
        # Clear tables for server and client
        _clear_tables(TestPostgresDatastore.server_dbname)
        _clear_tables(TestPostgresDatastore.client_dbname)
        _clear_tables(TestPostgresDatastore.third_dbname)

        self.server = PostgresDatastore(
            'server', TestPostgresDatastore.server_connstr, 'docs1')
        self.server.__enter__()
        self.client = PostgresDatastore(
            'client', TestPostgresDatastore.client_connstr, 'docs2')
        self.client.__enter__()
        self.third = PostgresDatastore(
            'third', TestPostgresDatastore.third_connstr, 'docs1')
        self.third.__enter__()

    def tearDown(self) -> None:
        self.server.__exit__()
        self.client.__exit__()
        self.third.__exit__()
