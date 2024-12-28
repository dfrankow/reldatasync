# pylint: disable=too-many-lines
import logging
import os
import random
import sqlite3
import unittest
from abc import abstractmethod
from unittest import SkipTest

import psycopg2
from reldatasync import util
from reldatasync.datastore import (
    Datastore,
    MemoryDatastore,
    NoSuchTable,
    PostgresDatastore,
    SqliteDatastore,
)
from reldatasync.document import _DELETED, _ID, _REV, _SEQ, Document
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
            self.skipTest("Skip base class test (_TestDatastore)")

    def assert_equals_no_seq(self, ds1, ds2):
        self.assertTrue(ds1.equals_no_seq(ds2))

    def sync(self, ds1: Datastore, ds2: Datastore):
        Replicator(ds1, ds2).sync_both_directions()

    def check(self, ds1: Datastore, ds2: Datastore):
        self.assertTrue(ds1.equals_no_seq(ds2))
        self.assertTrue(ds1.check())
        self.assertTrue(ds2.check())

    def sync_and_check(self, ds1: Datastore, ds2: Datastore):
        Replicator(ds1, ds2).sync_both_directions()
        self.check(ds1, ds2)

    def _get_datastore(self, ds, table):
        if self.server.__class__ == MemoryDatastore:
            ds = MemoryDatastore("name")
        elif self.server.__class__ == PostgresDatastore:
            ds = PostgresDatastore("name", ds.conn if ds else None, table)
        elif self.server.__class__ == SqliteDatastore:
            ds = SqliteDatastore("name", ds.conn if ds else None, table)
        return ds

    def test_datastore_bad_name(self):
        # datastore with a bad name returns an error
        table = "whoops"
        if self.server.__class__ == MemoryDatastore:
            # MemoryDatastore has no bad names
            pass
        elif self.server.__class__ == PostgresDatastore:
            ds = PostgresDatastore("name", self.server.conn, table)
            with self.assertRaises(NoSuchTable):
                # pylint: disable-next=unnecessary-dunder-call
                ds.__enter__()
            ds.conn.rollback()
            ds.__exit__()
            # The second time make sure we don't get "current transaction is aborted"
            ds = PostgresDatastore("name", self.server.conn, table)
            with self.assertRaises(NoSuchTable):
                # pylint: disable-next=unnecessary-dunder-call
                ds.__enter__()
            ds.conn.rollback()
            ds.__exit__()
        elif self.server.__class__ == SqliteDatastore:
            ds = SqliteDatastore("name", self.server.conn, table)
            with self.assertRaises(NoSuchTable):
                # pylint: disable-next=unnecessary-dunder-call
                ds.__enter__()
            ds.conn.rollback()
            ds.__exit__()
            # The second time make sure we don't get "current transaction is aborted"
            with self.assertRaises(NoSuchTable):
                # pylint: disable-next=unnecessary-dunder-call
                ds.__enter__()
            ds.conn.rollback()
            ds.__exit__()

    def test_datastore_id(self):
        # datastore without an id is assigned a random one
        ds = self._get_datastore(None, None)
        self.assertEqual(32, len(ds.id))
        self.assertNotIn("-", ds.id)

        # Don't check persistence of id (below) for MemoryDatastore
        if self.server.__class__ == MemoryDatastore:
            return

        # datastore id is assigned to a new datastore
        ds = self._get_datastore(self.server, "docs1")
        # need to use 'with' to execute __enter__
        with ds:
            id1 = ds.id
            self.assertEqual(32, len(ds.id))
            self.assertNotIn("-", ds.id)

        # If we make a new datastore with same name,
        # it has the same id as before
        ds = self._get_datastore(self.server, "docs1")
        with ds:
            self.assertEqual(id1, ds.id)

    def test_new_rev_and_seq(self):
        rev = ""
        rev, seq = self.server.new_rev_and_seq(rev)
        self.assertEqual(1, seq)
        self.assertEqual(str(VectorClock({"server_id": 1})), rev)

        rev, seq = self.server.new_rev_and_seq(rev)
        self.assertEqual(2, seq)
        self.assertEqual(str(VectorClock({"server_id": 2})), rev)

    def test_equals_no_seq(self):
        # server makes object A v1
        self.server.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)
        # client makes object A v1
        self.client.put(Document(**{_ID: "A", "value": "val2"}), increment_rev=True)

        # client updates object to be the same val1, but seq has changed
        self.client.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)

        sdoc = self.server.get("A")
        cdoc = self.client.get("A")
        self.assertNotEqual(sdoc, cdoc)
        self.assertEqual(1, sdoc.seq)
        self.assertEqual(2, cdoc.seq)
        self.assert_equals_no_seq(self.server, self.client)

    def test_nonoverlapping_sync(self):
        """Non-overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)
        # client makes object B v1
        self.client.put(Document(**{_ID: "B", "value": "val2"}), increment_rev=True)

        # sync leaves both server and client with A val1, B val2
        self.sync(self.client, self.server)

        # client
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 1})),
                    # A got put in client after B
                    _SEQ: 2,
                }
            ),
            self.client.get("A"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.client.get("B"),
        )

        # server
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.server.get("A"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    # B got put in server after A
                    _SEQ: 2,
                }
            ),
            self.server.get("B"),
        )

        # counter is at the highest existing doc version
        server_seq, server_docs = self.server.get_docs_since(0, 1000)
        self.assertEqual(self.server.sequence_id, server_seq)
        self.assertEqual(self.server.sequence_id, max(doc.seq for doc in server_docs))

        client_seq, client_docs = self.client.get_docs_since(0, 1000)
        self.assertEqual(self.client.sequence_id, client_seq)
        self.assertEqual(self.client.sequence_id, max(doc.seq for doc in client_docs))

        self.check(self.client, self.server)

    def test_put_if_needed(self):
        """put_if_needed doesn't put a second time"""
        doc = Document(**{_ID: "A", "value": "val1"})
        # put the doc
        num, doc = self.server.put(doc, increment_rev=True)
        self.assertEqual(1, num)
        # get doc back out with its _REV set
        self.assertTrue(doc.rev)
        # doc is already present, so it's not put again
        self.assertEqual(0, self.server.put(doc)[0])

        # doc is already present, but we said we changed it, so it's put
        doc.value = "val2"
        num, new_doc = self.server.put(doc, increment_rev=True)
        self.assertEqual(1, num)
        self.assertEqual("val2", new_doc.value)
        self.assertEqual(self.server.get("A"), new_doc)

    def test_overlapping_sync(self):
        """Overlapping documents from datastore"""
        # server makes object A v1
        self.server.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)
        self.server.put(Document(**{_ID: "C", "value": "val3"}), increment_rev=True)
        # client makes object B v1
        self.client.put(Document(**{_ID: "B", "value": "val2"}), increment_rev=True)
        self.client.put(Document(**{_ID: "C", "value": "val4"}), increment_rev=True)

        # sync leaves both server and client with A val1,  B val2, C val4
        self.sync_and_check(self.client, self.server)

        # client
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 1})),
                    _SEQ: 3,
                }
            ),
            self.client.get("A"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.client.get("B"),
        )
        # server's C won
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val3",
                    _REV: str(VectorClock({self.server.id: 2})),
                    _SEQ: 4,
                }
            ),
            self.client.get("C"),
        )

        # server
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.server.get("A"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    _SEQ: 3,
                }
            ),
            self.server.get("B"),
        )
        # server's C won
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val3",
                    _REV: str(VectorClock({self.server.id: 2})),
                    # server ignored client, so _SEQ is still 2
                    _SEQ: 2,
                }
            ),
            self.server.get("C"),
        )

    def test_get_docs_since(self):
        self.server.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)
        self.server.put(Document(**{_ID: "C", "value": "val3"}), increment_rev=True)
        doca = self.server.get("A")
        docc = self.server.get("C")
        # since 0 returns all the docs, in order
        current_seq = 2
        self.assertEqual((current_seq, [doca, docc]), self.server.get_docs_since(0, 10))
        # since 1 doesn't return doca
        self.assertEqual((current_seq, [docc]), self.server.get_docs_since(1, 10))

        # get_docs_since returns deleted docs
        self.server.delete("A")
        doca = self.server.get("A", include_deleted=True)
        current_seq = 3
        docs = self.server.get_docs_since(0, 10)
        self.assertEqual(
            # order switched (docc first), since deleting A increased version
            (current_seq, [docc, doca]),
            docs,
            docs,
        )

    def test_delete_sync(self):
        """Test that deletes get through syncing"""
        # server makes object A v1
        self.server.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)
        self.server.put(Document(**{_ID: "C", "value": "val3"}), increment_rev=True)
        # client makes object B v1
        self.client.put(Document(**{_ID: "B", "value": "val2"}), increment_rev=True)
        self.client.put(Document(**{_ID: "C", "value": "val4"}), increment_rev=True)

        # delete some
        self.server.delete("A")
        self.client.delete("C")

        # sync leaves both server and client with the same stuff
        self.sync_and_check(self.client, self.server)

        # client
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 3})),
                    _SEQ: 4,
                    _DELETED: True,
                }
            ),
            self.client.get("A", include_deleted=True),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.client.get("B"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val4",
                    _REV: str(VectorClock({self.client.id: 3})),
                    # client ignores server's change, so _SEQ is still 2
                    _SEQ: 3,
                    _DELETED: True,
                }
            ),
            self.client.get("C", include_deleted=True),
        )

        # server
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 3})),
                    _SEQ: 3,
                    _DELETED: True,
                }
            ),
            self.server.get("A", include_deleted=True),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    _SEQ: 4,
                }
            ),
            self.server.get("B"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val4",
                    # server get's client's change
                    _REV: str(VectorClock({self.client.id: 3})),
                    _SEQ: 5,
                    _DELETED: True,
                }
            ),
            self.server.get("C", include_deleted=True),
        )

    def test_delete_sync2(self):
        """Test a particular case that failed previously."""

        # Put items into server that will be ignored on client
        # a is in last, to have a higher sequence number than server will
        for item_name in ["i1", "a"]:
            self.client.put(
                Document(**{_ID: item_name, "value": 820}), increment_rev=True
            )
            self.client.put(
                Document(**{_ID: item_name, "value": 716}), increment_rev=True
            )

        # sync leaves both server and client with a
        self.sync_and_check(self.client, self.server)

        # delete on server
        self.server.delete("a")

        # sync leaves both server and client with deleted a
        self.sync_and_check(self.client, self.server)

    def test_three_servers(self):
        # If we have three servers A, B, C
        # and A syncs with B, B with C, but A never syncs with C
        # we should still have all three servers agree
        # server makes object A v1
        self.server.put(Document(**{_ID: "A", "value": "val1"}), increment_rev=True)
        self.server.put(Document(**{_ID: "D", "value": "val3"}), increment_rev=True)
        # client makes object B v1
        self.client.put(Document(**{_ID: "B", "value": "val2"}), increment_rev=True)
        self.client.put(Document(**{_ID: "D", "value": "val4"}), increment_rev=True)
        # third makes object C v1
        self.third.put(Document(**{_ID: "C", "value": "val3"}), increment_rev=True)
        self.third.put(Document(**{_ID: "D", "value": "val5"}), increment_rev=True)

        # pull server <= client
        logger.debug("*** pull server <= client")
        Replicator(self.server, self.client).pull_changes()
        # pull client <= third
        logger.debug("*** pull client <= third")
        Replicator(self.client, self.third).pull_changes()
        # pull server <= client
        logger.debug("*** pull server <= client")
        Replicator(self.server, self.client).pull_changes()

        # third only has C and D, since nothing pushed to it
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val3",
                    _REV: str(VectorClock({self.third.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.third.get("C"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "D",
                    "value": "val5",
                    _REV: str(VectorClock({self.third.id: 2})),
                    _SEQ: 2,
                }
            ),
            self.third.get("D"),
        )

        # now server has all of third's docs even though they never synced,
        # because server got third's changes through client
        for item in ("A", "B", "C", "D"):
            self.assertTrue(self.server.get(item))

        # server
        self.assertEqual(
            Document(
                **{
                    _ID: "A",
                    "value": "val1",
                    _REV: str(VectorClock({self.server.id: 1})),
                    _SEQ: 1,
                }
            ),
            self.server.get("A"),
        )
        self.assertEqual(
            Document(
                **{
                    _ID: "B",
                    "value": "val2",
                    _REV: str(VectorClock({self.client.id: 1})),
                    _SEQ: 3,
                }
            ),
            self.server.get("B"),
        )
        # This only succeeds if C traveled from third to client to server!
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val3",
                    _REV: str(VectorClock({self.third.id: 1})),
                    _SEQ: 4,
                }
            ),
            self.server.get("C"),
        )
        # server's D wins
        self.assertEqual(
            Document(
                **{
                    _ID: "D",
                    "value": "val3",
                    _REV: str(VectorClock({self.server.id: 2})),
                    # ignored third's D
                    _SEQ: 2,
                }
            ),
            self.server.get("D"),
        )

        # client also has C
        self.assertEqual(
            Document(
                **{
                    _ID: "C",
                    "value": "val3",
                    _REV: str(VectorClock({self.third.id: 1})),
                    _SEQ: 3,
                }
            ),
            self.client.get("C"),
        )

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
                datastore.put(Document(**{_ID: item, "value": val}), increment_rev=True)

    def test_long_streaks(self):
        items = [f"item{num}" for num in range(100)]

        for _ in range(16):
            # some mods for server, client, third
            # shuffle them so that one doesn't always win (by highest seq)
            dss = [self.server, self.client, self.third]
            random.shuffle(dss)
            for ds in dss:
                _TestDatastore._some_datastore_mods(ds, items)

            # sync in pairwise steps between the three datastores
            all_pairs = [
                [self.client, self.server],
                [self.client, self.third],
                [self.server, self.third],
            ]
            random.shuffle(all_pairs)
            for pair in all_pairs:
                Replicator(
                    pair[0],
                    pair[1],
                    # use small chunk size to test multiple chunks
                    chunk_size=2,
                ).sync_both_directions()

            # server and client should now contain the same stuff
            self.assert_equals_no_seq(self.client, self.server)
            self.assert_equals_no_seq(self.client, self.third)
            self.assert_equals_no_seq(self.server, self.third)
            self.assertTrue(self.client.check())
            self.assertTrue(self.server.check())
            self.assertTrue(self.third.check())

    def test_copy(self):
        doc = Document(**{_ID: "A", "value": "val1"})
        self.server.put(doc, increment_rev=True)
        doc.another = "foo"
        doc2 = self.server.get("A")
        self.assertTrue("another" not in doc2.model_dump().keys())
        self.assertTrue("another" in doc.model_dump().keys())

    def test_delete(self):
        doc = Document(**{_ID: "A", "value": "val1"})
        self.server.put(doc, increment_rev=True)
        doc1 = self.server.get("A")
        self.assertTrue(doc1)
        self.server.delete("A")

        # get doesn't return deleted doc by default
        self.assertIsNone(self.server.get("A"))

        # get returns deleted doc if asked
        doc2 = self.server.get("A", include_deleted=True)
        self.assertEqual(True, doc2.deleted)
        self.assertGreater(doc2.rev, doc1.rev)


class TestMemoryDatastore(_TestDatastore):
    def setUp(self):
        super().setUp()
        self.server = MemoryDatastore("server", "server_id")
        self.client = MemoryDatastore("client", "client_id")
        self.third = MemoryDatastore("third", "third_id")


class _TestDatabase:
    def __init__(self, dbname, dsname):
        """
        :param dbname:   Name of the database
        :param dsname:   Name of the datastore
        """
        self.dbname = dbname
        self.dsname = dsname

        self._conn = None
        self.datastore = None

    def close(self):
        if self.datastore:
            self.datastore.__exit__()
            self.datastore = None

        if self._conn:
            self._conn.close()
            self._conn = None

    @abstractmethod
    def create_test_db_and_tables(self):
        pass

    def _create_table_if_not_exists(self, tablename: str, definition: str):
        def exec_func(curs):
            curs.execute(f"CREATE TABLE IF NOT EXISTS {tablename} ({definition})")

        self.exec_sql(exec_func, dbname=self.dbname)

    def _create_test_tables(self):
        def exec_func(_curs):
            self._create_table_if_not_exists(
                "data_sync_revisions",
                "datastore_id varchar(100) not null,"
                "datastore_name varchar(1000) not null,"
                " sequence_id int not null",
            )
            # docs1 only needed on server, and docs2 on client
            # but it's easier to just create both tables on both
            docs_def = """
                _id text UNIQUE not null,
                _rev varchar(255) not null,
                _seq int not null,
                _deleted bool,
                value text
            """
            self._create_table_if_not_exists("docs1", docs_def)
            self._create_table_if_not_exists("docs2", docs_def)

        self.exec_sql(exec_func, dbname=self.dbname)

    def clear_and_reset_tables(self):
        logger.debug(f"clear_and_reset_tables {self.dbname}")

        def exec_func(curs):
            # reset sequence_id so tests start from 0
            # this breaks the abstraction barrier, but means the datastore
            # classes don't have to do twisted things just for testing
            curs.execute("UPDATE data_sync_revisions SET sequence_id = 0")
            curs.execute("DELETE FROM docs1")
            curs.execute("DELETE FROM docs2")

        self.exec_sql(exec_func, dbname=self.dbname)

    @abstractmethod
    def drop_db(self):
        pass

    @abstractmethod
    def exec_sql(self, exec_func, dbname=None, autocommit=True):
        pass


class _SqliteTestDatabase(_TestDatabase):
    # TODO: factor out connect
    def connect(self, table, datastore_id=None):
        if datastore_id is None:
            datastore_id = self.dsname + "_id"
        self._conn = sqlite3.connect(
            self.dbname,
            # Use autocommit to not need transactions:
            isolation_level=None,
        )
        self.datastore = SqliteDatastore(
            self.dsname, self._conn, table, datastore_id=datastore_id
        )
        # pylint: disable-next=unnecessary-dunder-call
        self.datastore.__enter__()

    def create_test_db_and_tables(self):
        # TODO: factor this out
        self._create_test_tables()

    def exec_sql(self, exec_func, dbname=None, autocommit=True):
        """Execute some SQL.

        Sometimes we want it with dbname, sometimes without (e.g., when
           creating the database).
        Otherwise we'd just keep a cursor around instead.
        """
        conn = None
        cursor = None
        try:
            conn = sqlite3.connect(self.dbname)
            logger.debug(f"Connect 1 {self.dbname}")
            cursor = conn.cursor()
            exec_func(cursor)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                logger.debug(f"Closed conn 1 {self.dbname}")

    def drop_db(self):
        # remove the sqlite file
        os.remove(self.dbname)


class _PostgresTestDatabase(_TestDatabase):
    # TODO: factor out connect
    def connect(self, table, datastore_id=None):
        if datastore_id is None:
            datastore_id = self.dsname + "_id"
        self._conn = psycopg2.connect(self._dbconnstr(self.dbname))
        # If we want to test with autocommit:
        # self._conn.autocommit = True
        self.datastore = PostgresDatastore(
            self.dsname, self._conn, table, datastore_id=datastore_id
        )
        # pylint: disable-next=unnecessary-dunder-call
        self.datastore.__enter__()

    @staticmethod
    def _dbconnstr(dbname=None):
        """Connect to test database on host db, user postgres by default.

        You can change host and server with environment variables POSTGRES_HOST
        and POSTGRES_SERVER.
        """
        host = os.getenv("POSTGRES_HOST", "localhost")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", None)
        the_str = f"host={host} user={user}"
        if dbname:
            the_str += f" dbname={dbname}"
        if password:
            the_str += f" password={password}"
        return the_str

    def exec_sql(self, exec_func, dbname=None, autocommit=True):
        """Execute some SQL.

        Sometimes we want it with dbname, sometimes without (e.g., when
           creating the database).
        Otherwise we'd just keep a cursor around instead.
        """
        connstr = self._dbconnstr(dbname)
        conn = None
        # NOTE: This particular style of executing a command is to allow
        # creating a database with later versions of psycopg2.
        # See https://stackoverflow.com/a/68112827
        ret = None
        try:
            conn = psycopg2.connect(connstr)
            logger.debug(f"Connect 1 {connstr}")
            if autocommit:
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                ret = exec_func(cursor)
        finally:
            if conn:
                conn.close()
                logger.debug(f"Closed conn 1 {connstr}")
        return ret

    def drop_db(self):
        self.exec_sql(lambda curs: curs.execute(f"DROP DATABASE {self.dbname}"))

    def _database_exists(self, database_name):
        def exec_func(curs):
            curs.execute(
                "SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
                (database_name,),
            )

            return bool(curs.fetchone())

        return self.exec_sql(exec_func, dbname=None, autocommit=True)

    def create_test_db_and_tables(self):
        self._create_test_db()
        self._create_test_tables()

    def _create_test_db(self):
        def exec_func(curs):
            if not self._database_exists(self.dbname):
                curs.execute(f"CREATE DATABASE {self.dbname}")
            else:
                logger.info(f"database {self.dbname} already exists")

        self.exec_sql(exec_func, dbname=None, autocommit=True)


# pylint: disable-next=too-many-instance-attributes
class _TestDatabases:
    SERVER_DBNAME = "rds_test_server"
    CLIENT_DBNAME = "rds_test_client"
    THIRD_DBNAME = "rds_test_third"

    def __init__(self, testdbclass):
        # dbname is the database in which the datastore will live
        self.server_dbname = self.SERVER_DBNAME
        self.client_dbname = self.CLIENT_DBNAME
        self.third_dbname = self.THIRD_DBNAME

        # dsname is the name of the datastore, no matter where it lives
        self.server_dsname = "server"
        self.client_dsname = "client"
        self.third_dsname = "third"

        # SAME_DB: if True, put server/client tables in one DB;
        # else put one table in two DBs.
        self.same_db = True

        if self.same_db:
            self.client_dbname = self.server_dbname

        self.testdbclass = testdbclass

        self.serverdb = None
        self.clientdb = None
        self.thirddb = None

        # Datastores
        self.client = None
        self.server = None
        self.third = None

    def init_dbclass(self):
        logger.debug("Set up server, client, third")
        self.serverdb = self.testdbclass(self.server_dbname, self.server_dsname)
        self.clientdb = self.testdbclass(self.client_dbname, self.client_dsname)
        self.thirddb = self.testdbclass(self.third_dbname, self.third_dsname)

    def connect(self):
        self.serverdb.connect("docs1")
        self.clientdb.connect(
            "docs2",
            # Even if client is connected to serverdb,
            # it's a different datastore
            datastore_id="client_id",
        )
        self.thirddb.connect("docs1")

        self.server = self.serverdb.datastore
        self.client = self.clientdb.datastore
        self.third = self.thirddb.datastore

    def _create_databases(self):
        # create server and client databases
        self.serverdb.create_test_db_and_tables()
        self.thirddb.create_test_db_and_tables()

        if not self.same_db:
            self.clientdb.create_test_db_and_tables()

        # self.connect()

    def _drop_databases(self):
        self.serverdb.drop_db()
        if not self.same_db:
            self.clientdb.drop_db()
        self.thirddb.drop_db()

    def reconnect_dbs(self, deep=False):
        self.close_connections()
        if deep:
            self._drop_databases()
            self._create_databases()
        self.connect()

    def close_connections(self):
        """Close connections"""
        logger.debug("Exit server, client, third")
        if self.server:
            # This __exit__ will be called by the close() below?
            # self.server.__exit__()
            self.server = None
        if self.client:
            # self.client.__exit__()
            self.client = None
        if self.third:
            # self.third.__exit__()
            self.third = None

        logger.debug("Set serverdb, clientdb, thirddb to None")
        for db_name in ["serverdb", "clientdb", "thirddb"]:
            db = getattr(self, db_name)
            if db:
                db.close()
                # Don't set to None as we can still use dbname
                # setattr(self, db_name, None)


class _TestDatabaseDatastore(_TestDatastore):
    _testdbs = None
    _testdbclass = None
    _deep_reconnect = False

    @classmethod
    def setUpClass(cls):
        if cls == _TestDatabaseDatastore:
            # Skipping here allows us to derive from _TestDatabaseDatastore
            # See also https://stackoverflow.com/a/35304339
            raise SkipTest("Skip base class test (_TestDatabaseDatastore)")

        super().setUpClass()
        assert cls._testdbs is None
        cls._testdbs = _TestDatabases(cls._testdbclass)
        cls._testdbs.init_dbclass()
        cls._testdbs._create_databases()
        cls._testdbs.connect()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls._testdbs.close_connections()
        cls._testdbs._drop_databases()

    def setUp(self):
        super().setUp()

        # Clear tables
        self._testdbs.serverdb.clear_and_reset_tables()
        self._testdbs.clientdb.clear_and_reset_tables()
        self._testdbs.thirddb.clear_and_reset_tables()

        # Put values back in tables
        self._testdbs.init_dbclass()

        # HACK: re-connect in order to reset sequence_id back to 0.
        # I could maybe do this directly with the datastore, but this works.
        # It also clears the tables, so I don't need to run the clear above.
        self._testdbs.reconnect_dbs(deep=self._deep_reconnect)

        # Allow the test to reference the test databases:
        self.server = self._testdbs.server
        self.client = self._testdbs.client
        self.third = self._testdbs.third


class TestPostgresDatastore(_TestDatabaseDatastore):
    _testdbclass = _PostgresTestDatabase


class TestSqliteDatastore(_TestDatabaseDatastore):
    _testdbclass = _SqliteTestDatabase
    # sqlite needs more work to reset sequences
    _deep_reconnect = True
