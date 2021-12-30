import logging
import os
import random

import psycopg2
import unittest

from reldatasync import util
from reldatasync.datastore import (
    MemoryDatastore, PostgresDatastore, Datastore)
from reldatasync.document import Document, _REV, _ID, _SEQ, _DELETED
from reldatasync.replicator import Replicator
from reldatasync.vectorclock import VectorClock

logger = logging.getLogger(__name__)

# Get log level from environment so we can set it for python -m unittest
util.logging_basic_config()


class _TestDatastore(unittest.TestCase):
    """Base class for testing datastores."""

    def setUp(self):
        self.server = None
        self.client = None
        self.third = None

        if self.__class__ == _TestDatastore:
            # Skipping here allows us to derive _TestDatastore from TestCase
            # See also https://stackoverflow.com/a/35304339
            self.skipTest('Skip base class test (_TestDatastore)')

    def assert_equals_no_seq(self, ds1, ds2):
        self.assertTrue(ds1.equals_no_seq(ds2))

    def sync_and_check(self, ds1: Datastore, ds2: Datastore):
        Replicator(ds1, ds2).sync_both_directions()
        self.assertTrue(ds1.equals_no_seq(ds2))
        self.assertTrue(ds1.check())
        self.assertTrue(ds2.check())

    def test_datastore_id(self):
        if self.server.__class__ == MemoryDatastore:
            ds = MemoryDatastore('name')
        elif self.server.__class__ == PostgresDatastore:
            ds = PostgresDatastore('name', None, 'table')
        self.assertEqual(32, len(ds.id))
        self.assertNotIn('-', ds.id)

    def test_new_rev_and_seq(self):
        rev = ''
        rev, seq = self.server.new_rev_and_seq(rev)
        self.assertEqual(1, seq)
        self.assertEqual(str(VectorClock({'server_id': 1})), rev)

        rev, seq = self.server.new_rev_and_seq(rev)
        self.assertEqual(2, seq)
        self.assertEqual(str(VectorClock({'server_id': 2})), rev)

    def test_nonoverlapping_sync(self):
        """Non-overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(
            Document({_ID: 'A', 'value': 'val1'}), increment_rev=True)
        # client makes object B v1
        self.client.put(
            Document({_ID: 'B', 'value': 'val2'}), increment_rev=True)

        # sync leaves both server and client with A val1, B val2
        self.sync_and_check(self.client, self.server)

        # client
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 1})),
                      # A got put in client after B
                      _SEQ: 2}),
            self.client.get('A'))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      _SEQ: 1}),
            self.client.get('B'))

        # server
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 1})),
                      _SEQ: 1}),
            self.server.get('A'))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      # B got put in server after A
                      _SEQ: 2}),
            self.server.get('B'))

        # counter is at the highest existing doc version
        server_seq, server_docs = self.server.get_docs_since(0, 1000)
        self.assertEqual(self.server.sequence_id, server_seq)
        self.assertEqual(self.server.sequence_id,
                         max(doc[_SEQ] for doc in server_docs))

        client_seq, client_docs = self.client.get_docs_since(0, 1000)
        self.assertEqual(self.client.sequence_id, client_seq)
        self.assertEqual(self.client.sequence_id,
                         max(doc[_SEQ] for doc in client_docs))

    def test_put_if_needed(self):
        """put_if_needed doesn't put a second time"""
        doc = Document({_ID: 'A', 'value': 'val1'})
        # put the doc
        num, doc = self.server.put(doc, increment_rev=True)
        self.assertEqual(1, num)
        # get doc back out with its _REV set
        self.assertTrue(doc[_REV])
        # doc is already present, so it's not put again
        self.assertEqual(0, self.server.put(doc)[0])

        # doc is already present, but we said we changed it, so it's put
        doc['value'] = 'val2'
        num, new_doc = self.server.put(doc, increment_rev=True)
        self.assertEqual(1, num)
        self.assertEqual('val2', new_doc['value'])

    def test_overlapping_sync(self):
        """Overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(
            Document({_ID: 'A', 'value': 'val1'}), increment_rev=True)
        self.server.put(
            Document({_ID: 'C', 'value': 'val3'}), increment_rev=True)
        # client makes object B v1
        self.client.put(
            Document({_ID: 'B', 'value': 'val2'}), increment_rev=True)
        self.client.put(
            Document({_ID: 'C', 'value': 'val4'}), increment_rev=True)

        # sync leaves both server and client with A val1,  B val2, C val4
        self.sync_and_check(self.client, self.server)

        # client
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 1})),
                      _SEQ: 3}),
            self.client.get('A'))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      _SEQ: 1}),
            self.client.get('B'))
        # server's C won
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val3',
                      _REV: str(VectorClock({self.server.id: 2})),
                      _SEQ: 4}),
            self.client.get('C'))

        # server
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 1})),
                      _SEQ: 1}),
            self.server.get('A'))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      _SEQ: 3}),
            self.server.get('B'))
        # server's C won
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val3',
                      _REV: str(VectorClock({self.server.id: 2})),
                      # server ignored client, so _SEQ is still 2
                      _SEQ: 2}),
            self.server.get('C'))

    def test_get_docs_since(self):
        self.server.put(
            Document({_ID: 'A', 'value': 'val1'}), increment_rev=True)
        self.server.put(
            Document({_ID: 'C', 'value': 'val3'}), increment_rev=True)
        doca = self.server.get('A')
        docc = self.server.get('C')
        # since 0 returns all the docs, in order
        current_seq = 2
        self.assertEqual(
            (current_seq, [doca, docc]),
            self.server.get_docs_since(0, 10))
        # since 1 doesn't return doca
        self.assertEqual(
            (current_seq, [docc]),
            self.server.get_docs_since(1, 10))

        # get_docs_since returns deleted docs
        self.server.delete('A')
        doca = self.server.get('A', include_deleted=True)
        current_seq = 3
        self.assertEqual(
            # order switched (docc first), since deleting A increased version
            (current_seq, [docc, doca]),
            self.server.get_docs_since(0, 10))

    def test_delete_sync(self):
        """Test that deletes get through syncing"""
        # server makes object A v1
        self.server.put(
            Document({_ID: 'A', 'value': 'val1'}), increment_rev=True)
        self.server.put(
            Document({_ID: 'C', 'value': 'val3'}), increment_rev=True)
        # client makes object B v1
        self.client.put(
            Document({_ID: 'B', 'value': 'val2'}), increment_rev=True)
        self.client.put(
            Document({_ID: 'C', 'value': 'val4'}), increment_rev=True)

        # delete some
        self.server.delete('A')
        self.client.delete('C')

        # sync leaves both server and client with the same stuff
        self.sync_and_check(self.client, self.server)

        # client
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 3})),
                      _SEQ: 4,
                      _DELETED: True}),
            self.client.get('A', include_deleted=True))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      _SEQ: 1}),
            self.client.get('B'))
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val4',
                      _REV: str(VectorClock({self.client.id: 3})),
                      # client ignores server's change, so _SEQ is still 2
                      _SEQ: 3,
                      _DELETED: True}),
            self.client.get('C', include_deleted=True))

        # server
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 3})),
                      _SEQ: 3,
                      _DELETED: True}),
            self.server.get('A', include_deleted=True))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      _SEQ: 4}),
            self.server.get('B'))
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val4',
                      # server get's client's change
                      _REV: str(VectorClock({self.client.id: 3})),
                      _SEQ: 5,
                      _DELETED: True}),
            self.server.get('C', include_deleted=True))

    def test_delete_sync2(self):
        """Test a particular case that failed previously."""

        # Put items into server that will be ignored on client
        # a is in last, to have a higher sequence number than server will
        for item_name in ['i1', 'a']:
            self.client.put(
                Document({_ID: item_name, 'value': 820}), increment_rev=True)
            self.client.put(
                Document({_ID: item_name, 'value': 716}), increment_rev=True)

        # sync leaves both server and client with a
        self.sync_and_check(self.client, self.server)

        # delete on server
        self.server.delete('a')

        # sync leaves both server and client with deleted a
        self.sync_and_check(self.client, self.server)

    def test_three_servers(self):
        # If we have three servers A, B, C
        # and A syncs with B, B with C, but A never syncs with C
        # we should still have all three servers agree
        # server makes object A v1
        self.server.put(
            Document({_ID: 'A', 'value': 'val1'}), increment_rev=True)
        self.server.put(
            Document({_ID: 'D', 'value': 'val3'}), increment_rev=True)
        # client makes object B v1
        self.client.put(
            Document({_ID: 'B', 'value': 'val2'}), increment_rev=True)
        self.client.put(
            Document({_ID: 'D', 'value': 'val4'}), increment_rev=True)
        # third makes object C v1
        self.third.put(
            Document({_ID: 'C', 'value': 'val3'}), increment_rev=True)
        self.third.put(
            Document({_ID: 'D', 'value': 'val5'}), increment_rev=True)

        # pull server <= client
        logger.debug('*** pull server <= client')
        Replicator(self.server, self.client).pull_changes()
        # pull client <= third
        logger.debug('*** pull client <= third')
        Replicator(self.client, self.third).pull_changes()
        # pull server <= client
        logger.debug('*** pull server <= client')
        Replicator(self.server, self.client).pull_changes()

        # third only has C and D, since nothing pushed to it
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val3',
                      _REV: str(VectorClock({self.third.id: 1})),
                      _SEQ: 1}),
            self.third.get('C'))
        self.assertEqual(
            Document({_ID: 'D', 'value': 'val5',
                      _REV: str(VectorClock({self.third.id: 2})),
                      _SEQ: 2}),
            self.third.get('D'))

        # now server has all of third's docs even though they never synced,
        # because server got third's changes through client
        for item in ('A', 'B', 'C', 'D'):
            self.assertTrue(self.server.get(item))

        # server
        self.assertEqual(
            Document({_ID: 'A', 'value': 'val1',
                      _REV: str(VectorClock({self.server.id: 1})),
                      _SEQ: 1}),
            self.server.get('A'))
        self.assertEqual(
            Document({_ID: 'B', 'value': 'val2',
                      _REV: str(VectorClock({self.client.id: 1})),
                      _SEQ: 3}),
            self.server.get('B'))
        # This only succeeds if C traveled from third to client to server!
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val3',
                      _REV: str(VectorClock({self.third.id: 1})),
                      _SEQ: 4}),
            self.server.get('C'))
        # server's D wins
        self.assertEqual(
            Document({_ID: 'D', 'value': 'val3',
                      _REV: str(VectorClock({self.server.id: 2})),
                      # ignored third's D
                      _SEQ: 2}),
            self.server.get('D'))

        # client also has C
        self.assertEqual(
            Document({_ID: 'C', 'value': 'val3',
                      _REV: str(VectorClock({self.third.id: 1})),
                      _SEQ: 3}),
            self.client.get('C'))

    @staticmethod
    def _some_datastore_mods(datastore, items):
        num_steps = random.randint(2, 30)
        for _ in range(num_steps):
            # pick item
            item = random.choice(items)
            if random.uniform(0, 1) < 0.3:
                datastore.delete(item)
            else:
                val = random.randint(0, 1000)
                datastore.put(
                    Document({_ID: item, 'value': val}), increment_rev=True)

    def test_long_streaks(self):
        items = [f'item{num}' for num in range(100)]

        for _ in range(16):
            # some mods for server, client, third
            # shuffle them so that one doesn't always win (by highest seq)
            dss = [self.server, self.client, self.third]
            random.shuffle(dss)
            for ds in dss:
                _TestDatastore._some_datastore_mods(ds, items)

            # sync in pairwise steps between the three datastores
            all_pairs = [[self.client, self.server],
                         [self.client, self.third],
                         [self.server, self.third]]
            random.shuffle(all_pairs)
            for pair in all_pairs:
                Replicator(pair[0], pair[1],
                           # use small chunk size to test multiple chunks
                           chunk_size=2).sync_both_directions()

            # server and client should now contain the same stuff
            self.assert_equals_no_seq(self.client, self.server)
            self.assert_equals_no_seq(self.client, self.third)
            self.assert_equals_no_seq(self.server, self.third)
            self.assertTrue(self.client.check())
            self.assertTrue(self.server.check())
            self.assertTrue(self.third.check())

    def test_copy(self):
        doc = Document({_ID: 'A', 'value': 'val1'})
        self.server.put(doc, increment_rev=True)
        doc['another'] = 'foo'
        doc2 = self.server.get('A')
        self.assertTrue('another' not in doc2)
        self.assertTrue('another' in doc)

    def test_delete(self):
        doc = Document({_ID: 'A', 'value': 'val1'})
        self.server.put(doc, increment_rev=True)
        doc1 = self.server.get('A')
        self.assertTrue(doc1)
        self.server.delete('A')

        # get doesn't return deleted doc by default
        self.assertIsNone(self.server.get('A'))

        # get returns deleted doc if asked
        doc2 = self.server.get('A', include_deleted=True)
        self.assertEqual(True, doc2['_deleted'])
        self.assertGreater(doc2[_REV], doc1[_REV])


class TestMemoryDatastore(_TestDatastore):
    def setUp(self):
        super().setUp()
        self.server = MemoryDatastore('server', 'server_id')
        self.client = MemoryDatastore('client', 'client_id')
        self.third = MemoryDatastore('third', 'third_id')


def _dbconnstr(dbname=None):
    """Connect to test database on host db, user postgres by default.

    You can change host and server with environment variables POSTGRES_HOST
    and POSTGRES_SERVER.
    """
    host = os.getenv('POSTGRES_HOST', 'localhost')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', None)
    the_str = f'host={host} user={user}'
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
        logger.debug(f'Connect 1 {connstr}')
        if autocommit:
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cursor:
            exec_func(cursor)
    finally:
        if conn:
            conn.close()
            logger.debug(f'Closed conn 1 {connstr}')


def _create_test_db(dbname):
    _create_test_db1(dbname)
    _create_test_tables(dbname)


def _create_test_db1(dbname):
    def exec_func(curs):
        curs.execute(
            'SELECT datname FROM pg_catalog.pg_database WHERE datname = %s',
            (dbname,))
        if not curs.fetchone():
            curs.execute('CREATE DATABASE %s' % dbname)
        else:
            logger.info('database %s already exists' % dbname)
    exec_sql(exec_func, autocommit=True)


def _create_table_if_not_exists(dbname: str, tablename: str,
                                definition: str):
    def exec_func(curs):
        # check for table existence
        curs.execute(
            'select * from information_schema.tables where table_name=%s',
            (tablename,))
        if not bool(curs.fetchone()):
            # table doesn't exist, create it
            curs.execute('CREATE TABLE %s (%s)' % (tablename, definition))
    exec_sql(exec_func, dbname=dbname)


def _create_test_tables(dbname):
    def exec_func(curs):
        _create_table_if_not_exists(
            dbname,
            'data_sync_revisions',
            'datastore_id varchar(100) not null,'
            'datastore_name varchar(1000) not null,'
            ' sequence_id int not null')
        # docs1 only needed on server, and docs2 on client
        # but it's easier to just create both tables on both
        docs_def = """
            _id text UNIQUE not null,
            _rev varchar(255) not null,
            _seq int not null,
            _deleted bool,
            value text
        """
        _create_table_if_not_exists(dbname, 'docs1', docs_def)
        _create_table_if_not_exists(dbname, 'docs2', docs_def)
    exec_sql(exec_func, dbname=dbname)


def _clear_tables(dbname):
    # logger.debug(f"_clear_tables {dbname}")

    def exec_func(curs):
        curs.execute('DELETE FROM data_sync_revisions')
        curs.execute('DELETE FROM docs1')
        curs.execute('DELETE FROM docs2')

        curs.execute('SELECT sequence_id FROM data_sync_revisions')
        result = curs.fetchone()
        assert result is None

    exec_sql(exec_func, dbname=dbname)


def _drop_db(dbname):
    exec_sql(lambda curs: curs.execute('DROP DATABASE %s' % dbname))


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

    cls.server_conn = psycopg2.connect(cls.server_connstr)
    cls.client_conn = psycopg2.connect(cls.client_connstr)
    cls.third_conn = psycopg2.connect(cls.third_connstr)


def _drop_databases(cls, same_db):
    _drop_db(cls.server_dbname)
    if not same_db:
        _drop_db(cls.client_dbname)
    _drop_db(cls.third_dbname)


class TestPostgresDatastore(_TestDatastore):
    # These aren't needed, but make warnings go away:
    server_dbname = None
    client_dbname = None
    third_dbname = None
    client_connstr = None
    server_connstr = None
    third_connstr = None
    client_conn = None
    server_conn = None
    third_conn = None

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

        # Close connections
        for conn_name in ['server_conn', 'client_conn', 'third_conn']:
            conn = getattr(cls, conn_name)
            if conn:
                logger.debug(f'Close conn 2 {conn_name}')
                conn.close()

        _drop_databases(cls, TestPostgresDatastore.SAME_DB)

    def setUp(self):
        super().setUp()

        # Clear tables for server and client
        _clear_tables(TestPostgresDatastore.server_dbname)
        _clear_tables(TestPostgresDatastore.client_dbname)
        _clear_tables(TestPostgresDatastore.third_dbname)

        # HACK: re-connect in order to get the changes from _clear_tables
        # If I knew what I was doing, the _clear_tables from above would
        # be seen in these connections without re-connecting
        cls = TestPostgresDatastore
        cls.server_conn.close()
        logger.debug('Closed conn 3 server_conn')
        cls.server_conn = psycopg2.connect(cls.server_connstr)
        logger.debug('Connected 3 conn server_conn')
        cls.client_conn.close()
        logger.debug('Closed conn 3 client_conn')
        cls.client_conn = psycopg2.connect(cls.client_connstr)
        logger.debug('Connected 3 conn client_conn')
        cls.third_conn.close()
        logger.debug('Closed conn 3 third_conn')
        cls.third_conn = psycopg2.connect(cls.third_connstr)
        logger.debug('Connected 3 conn third_conn')

        self.server = PostgresDatastore(
            'server', TestPostgresDatastore.server_conn, 'docs1',
            datastore_id='server_id')
        self.server.__enter__()
        self.client = PostgresDatastore(
            'client', TestPostgresDatastore.client_conn, 'docs2',
            datastore_id='client_id')
        self.client.__enter__()
        self.third = PostgresDatastore(
            'third', TestPostgresDatastore.third_conn, 'docs1',
            datastore_id='third_id')
        self.third.__enter__()

    def tearDown(self) -> None:
        self.server.__exit__()
        self.client.__exit__()
        self.third.__exit__()
