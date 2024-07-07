"""An abstraction of a datastore, to use for syncing."""
import functools
import logging
import sqlite3
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Sequence
from typing import Generic, Optional

import psycopg2
import requests
from reldatasync import util
from reldatasync.document import _DELETED, _ID, _REV, _SEQ, ID_TYPE, Document
from reldatasync.vectorclock import VectorClock

logger = logging.getLogger(__name__)


class Datastore(Generic[ID_TYPE], ABC):
    def __init__(self, datastore_name: str, datastore_id: Optional[str] = None):
        """Init a datastore.

        :param datastore_name:  Human-readable name
        :param datastore_id:  Unique identifier, used in revisions
                              Don't set the id unless you are sure.
                              If you have two datastores with the same id,
                              it won't be good.
        """
        self.name = datastore_name
        self.id = datastore_id
        if not self.id:
            self.id = util.uuid4_string()
        self._sequence_id = 0
        self.peer_seq_ids = {}

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def check(self, max_size=1000):
        """Do some sanity checks.  Return True if they pass.

        Note: this doesn't fully check if there are more than max_size docs.
        Note: this reads all docs into memory.
        """
        ret = True
        all_docids = set()
        all_seqs = set()
        max_seq, docs = self.get_docs_since(0, max_size)
        doc_max_seq = 0
        for doc in docs:
            docid = doc.get(_ID, "?")

            # check uniqueness of docid
            if docid in all_docids:
                logger.warning(f"docid {docid} has repeated id")
            all_docids.add(docid)

            # check docs have _ID, _REV, _SEQ
            for field in [_ID, _REV, _SEQ]:
                if field not in doc:
                    logger.warning(f"doc {docid} has no {field}")
                    ret = False

            # check doc _SEQ
            seq = doc.get(_SEQ, None)
            if seq in all_seqs:
                logger.warning(f"docid {docid} has repeated seq {seq}")
            all_seqs.add(seq)

            if seq > doc_max_seq:
                doc_max_seq = seq

            if not 0 < doc[_SEQ] <= max_seq:
                logger.warning(f"doc {docid} has seq out of bounds {seq}")
                ret = False

        if max_seq != doc_max_seq:
            logger.warning(f"doc_max_seq {doc_max_seq} max_seq reported {max_seq}")
            ret = False

        return ret

    def equals_no_seq(self, other: "Datastore", max_docs: int = 1000):
        """True if two datastores have the same docs, ignoring the _SEQ key.

        _SEQ is local to a datastore, it can differ due to 'last write wins'.

        This reads all docs from both datastores into memory.
        """
        _, docs1 = self.get_docs_since(0, max_docs)
        _, docs2 = other.get_docs_since(0, max_docs)

        if len(docs1) != len(docs2):
            logger.debug(f"len(docs1) = {len(docs1)} != len(docs2) = {len(docs2)}")
            return False

        def compare_no_seq(a, b):
            return a.compare(b, ignore_keys={_SEQ})

        docs1 = sorted(docs1, key=functools.cmp_to_key(compare_no_seq))
        docs2 = sorted(docs2, key=functools.cmp_to_key(compare_no_seq))
        if logger.isEnabledFor(logging.DEBUG):
            # pylint: disable-next=consider-using-enumerate
            for idx in range(len(docs1)):
                logger.debug(
                    f"{self.id} docs[{idx}]={docs1[idx]}\n"
                    f"{other.id} docs[{idx}]={docs2[idx]}\n"
                )

        # pylint: disable-next=consider-using-enumerate
        for idx in range(len(docs1)):
            if docs1[idx].compare(docs2[idx], ignore_keys={_SEQ}) != 0:
                logger.debug(
                    "First unequal element: "
                    f"{self.id} docs[{idx}]={docs1[idx]}\n"
                    f"{other.id} docs[{idx}]={docs2[idx]}"
                )
                return False

        return True

    def _increment_sequence_id(self) -> int:
        self._sequence_id += 1
        logger.debug(
            f"{self.id}: Increment {self.id}" f" _sequence_id to {self._sequence_id}"
        )
        return self._sequence_id

    def _set_sequence_id(self, the_id) -> None:
        """Set sequence id to the_id."""
        if the_id < self._sequence_id:
            raise ValueError(
                f"Setting sequence_id backwards,"
                f" from {self._sequence_id} to {the_id}"
            )
        self._sequence_id = the_id

    @property
    def sequence_id(self) -> int:
        """Read-only sequence_id"""
        return self._sequence_id

    def _set_new_rev(self, doc: Document, seq_id: int) -> None:
        """Set increment_rev revision for a doc."""
        rev = VectorClock.from_string(doc.get(_REV, "{}"))
        rev.set_clock(self.id, seq_id)
        doc[_REV] = str(rev)

    def new_rev_and_seq(self, rev_str):
        """Get a new rev and seq for use saving without the 'put' method."""
        seq_id = self._increment_sequence_id()

        if not rev_str:
            rev_str = "{}"
        rev = VectorClock.from_string(rev_str)
        rev.set_clock(self.id, seq_id)
        return str(rev), seq_id

    @abstractmethod
    def _put(self, doc: Document):
        pass

    def put(self, doc: Document, increment_rev=False) -> tuple[int, Document]:
        """Put doc under docid if rev is greater, or doc doesn't currently exist

        Return number of records actually put (0 or 1).

        As a side effect, this updates self.sequence_id if doc is put.

        :param doc  Document to put
        :param increment_rev  If True, increment revision.  If revision is
                              not present, it adds one.
        """
        if not increment_rev and _REV not in doc:
            raise ValueError(
                f"doc {doc.get(_ID, '')} must have {_REV}" f" if increment_rev is False"
            )

        assert doc.__class__ == Document, f"doc class is {doc.__class__}"

        # copy doc so we don't modify caller's doc
        doc = doc.copy()

        ret = 0
        docid = doc[_ID]

        rev_str = doc.get(_REV, None)
        if increment_rev:
            if rev_str is None:
                rev_str = "{}"
            rev = VectorClock.from_string(rev_str)
            # the new rev below would be >= to this:
            rev.set_clock(self.id, self.sequence_id + 1)
        else:
            # we threw a ValueError above if it was None
            assert rev_str is not None
            try:
                rev = VectorClock.from_string(rev_str)
            except ValueError as err:
                raise ValueError(f"{_REV} must be a JSON dictionary: {err}")

        my_doc = self.get(docid, include_deleted=True)

        my_rev = VectorClock.from_string(my_doc.get(_REV)) if my_doc else None
        if (my_rev is None) or (my_rev < rev):
            seq_id = self._increment_sequence_id()
            if increment_rev:
                # Now assign the rev for real
                # Maybe sequence id changed since we looked above,
                # use the one we just got
                rev.set_clock(self.id, seq_id)
                assert _REV not in doc or rev > VectorClock.from_string(
                    doc[_REV]
                ), "rev did not increase: {rev} !> {doc[_REV]} "
                doc[_REV] = str(rev)
            doc[_SEQ] = seq_id
            self._put(doc)
            ret = 1

            logger.debug(
                f"{self.id}: Put docid {docid} doc {doc} rev {rev}"
                f" inc_rev {increment_rev}"
                f" (compared to my_doc {my_doc} my_rev {my_rev})"
            )
        else:
            logger.debug(
                f"{self.id}: Ignore docid {docid} doc {doc} rev {rev}"
                f" inc_rec {increment_rev}"
                f" (compared to doc {my_doc} my_rev {my_rev})"
            )
        return ret, doc

    def delete(self, docid: ID_TYPE) -> None:
        """Delete an doc in the datastore.

        Returns silently if the doc is not in the datastore.
        """
        doc = self.get(docid)
        assert doc is None or _REV in doc
        if doc and not doc.get(_DELETED, False):
            doc[_DELETED] = True
            # Deletion makes a increment_rev rev
            seq_id = self._increment_sequence_id()
            doc[_SEQ] = seq_id
            self._set_new_rev(doc, seq_id)
            logger.debug(f"{self.id}: after delete {doc}")
            self._put(doc)

    def get_peer_sequence_id(self, peer: str) -> int:
        """Get the seq we have for peer, or zero if we have none."""
        return self.peer_seq_ids.get(peer, 0)

    def set_peer_sequence_id(self, peer: str, seq: int) -> None:
        """Set increment_rev peer sequence id, if seq > what we have."""
        if seq > self.get_peer_sequence_id(peer):
            logger.debug(f"{self.id}: set peer_seq_ids[{peer}] = {seq}")
            self.peer_seq_ids[peer] = seq

    @abstractmethod
    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        pass

    @abstractmethod
    def get_docs_since(self, the_seq: int, num: int) -> tuple[int, Sequence[Document]]:
        """Get docs put with the_seq < seq <= (the_seq+num).

        This is intended to be called repeatedly to get them all, so as to
        allow syncing in chunks.

        :return current sequence id, sequence of about "num" oldest docs

        The current sequence id is useful to know if we've reached the end
        of the list of updates needed.

        The oldest docs returned is about "num" docs, but may be fewer if
        there are holes in the seq ids of the docs, or more if there are lots
        of ties in sequence number from multiple docs that occurred
        simultaneously.

        After receiving these docs, the caller has all docs up to
        min(the_seq+num, current sequence id).
        """


class MemoryDatastore(Datastore):
    """An in-memory transient datastore, only useful for testing."""

    def __init__(self, datastore_name: str, datastore_id: Optional[str] = None):
        super().__init__(datastore_name, datastore_id)
        self.datastore = OrderedDict()

    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        """Return doc, or None if not present.

        :param docid  Doc id
        :param include_deleted  If True, don't return deleted item"""
        doc = self.datastore.get(docid, None)
        if doc:
            # Return a copy so our internals cannot be modified
            doc = doc.copy()
            # don't include deleted docs
            if not include_deleted and doc.get(_DELETED, False):
                logger.debug(f"Don't return deleted doc {doc}")
                doc = None
        return doc

    def _put(self, doc: Document) -> None:
        """Put doc under docid."""
        assert _REV in doc
        docid = doc[_ID]
        self.datastore[docid] = doc
        # preserve doc key order
        self.datastore.move_to_end(docid)

    def get_docs_since(self, the_seq: int, num: int) -> tuple[int, Sequence[Document]]:
        """Get docs put with the_seq < seq <= (the_seq+num).

        This is intended to be called repeatedly to get them all, so as to
        allow syncing in chunks.

        :return  current sequence id, docs
        """
        docs = []
        for _docid, doc in self.datastore.items():
            doc_seq = doc[_SEQ]
            assert doc_seq is not None
            if the_seq < doc_seq <= (the_seq + num):
                docs.append(doc)
            # Since self.datastore is ordered, we can cut out early
            # This is optional, perhaps premature optimization
            if doc_seq > (the_seq + num):
                break
        return self.sequence_id, docs


class NoSuchTable(Exception):
    pass


class DatabaseDatastore(Datastore, ABC):
    """Base datastore for a relational database."""

    def __init__(
        self,
        datastore_name: str,
        conn,
        tablename: str,
        datastore_id: Optional[str] = None,
    ):
        super().__init__(datastore_name, datastore_id)
        self.tablename = tablename
        self.conn = conn
        self.columnnames = None
        self.cursor = None

        # set in child class
        self.placeholder = None

    def _row_to_doc(self, docrow) -> Document:
        the_dict = {}
        assert len(docrow) == len(self.columnnames)
        # pylint: disable-next=consider-using-enumerate
        for idx in range(len(docrow)):
            the_dict[self.columnnames[idx]] = docrow[idx]
        # Treat '_deleted' specially: get rid of it if it's None
        if the_dict[_DELETED] is None:
            del the_dict[_DELETED]
        return Document(the_dict)

    def __enter__(self):
        super().__enter__()

        # TODO: What exactly is our commit policy?

        self.cursor = self.conn.cursor()

        # TODO: Create the data_sync_revisions table if needed?

        # Init sequence_id if not present
        # Check that the right tables exist
        self._init_datastore_id()

        # Get the column names for self.tablename
        try:
            self.cursor.execute(f"SELECT * FROM {self.tablename} LIMIT 0")
        # It's bad to catch "Exception", but Django re-wraps all the errors, so the
        # first level that is non-Django is "Exception"
        except (
            sqlite3.OperationalError,
            psycopg2.errors.UndefinedTable,
            Exception,
        ) as err:
            # We can't rollback because Django also manages the low-level connection
            # So, the client has to manage this
            # self.conn.rollback()
            raise NoSuchTable from err
        self.columnnames = [desc[0] for desc in self.cursor.description]

        # Check that self.tablename has _id, _deleted, and _rev
        for field in (_ID, _REV, _DELETED):
            if field not in self.columnnames:
                raise NameError(f"Field '{field}' not in table '{self.tablename}'")

        # TODO: Check self.tablename has a unique index on _id
        # Required for proper functioning of UPSERT

        # TODO: Check that sequence_id in revisions table is >= max(REV)
        #   in the data

        return self

    def __exit__(self, *args):
        super().__exit__()
        if self.cursor:
            self.cursor.close()

    def _init_datastore_id(self):
        """Init datastore id and sequence_id.

        Sets self.id and self._sequence_id.
        If an entry exists in the table use it, else initialize that entry.

        Also raises an exception if the table doesn't exist."""
        self.cursor.execute(
            "SELECT datastore_id, sequence_id FROM data_sync_revisions"
            f" WHERE datastore_name={self.placeholder}",
            (self.name,),
        )
        new_val = self.cursor.fetchone()
        if new_val:
            # Already an id, use it
            self.id = new_val[0]
            super()._set_sequence_id(new_val[1])
            logger.debug(
                f"set self.id to {self.id}," f" _sequence_id to {self._sequence_id}"
            )
        else:
            # No id, insert one
            assert self.id is not None
            self.cursor.execute(
                "INSERT INTO data_sync_revisions"
                " (datastore_id, datastore_name, sequence_id)"
                f" VALUES ({self.placeholder}, {self.placeholder}, 0)",
                (self.id, self.name),
            )
            super()._set_sequence_id(0)
            logger.debug(
                f"set self.id to {self.id}," f" _sequence_id to {self._sequence_id}"
            )

    # def _set_sequence_id(self, the_id) -> None:
    #     # SQLite started supporting RETURNING in version 3.35.0 (2021-03-12).
    #     # We want to support earlier sqlite versions, so we don't use it.
    #     # TODO: Test setting different sequence ids for different datastores
    #     self.cursor.execute(
    #         f"UPDATE data_sync_revisions set sequence_id = {self.placeholder}"
    #         f" WHERE datastore_id={self.placeholder}",
    #         (
    #             the_id,
    #             self.id,
    #         ),
    #     )
    #     self.cursor.execute(
    #         "SELECT sequence_id FROM data_sync_revisions"
    #         f" WHERE datastore_id={self.placeholder}",
    #         (self.id,),
    #     )
    #     new_val = self.cursor.fetchone()[0]
    #     super()._set_sequence_id(the_id)
    #     assert (
    #         self._sequence_id == new_val
    #     ), f"seq_id {self._sequence_id} DB seq_id {new_val}"

    def _increment_sequence_id(self) -> int:
        # SQLite started supporting RETURNING in version 3.35.0 (2021-03-12).
        # We want to support earlier sqlite versions, so we don't use it.
        self.cursor.execute(
            "UPDATE data_sync_revisions set sequence_id = sequence_id+1"
            f" WHERE datastore_id={self.placeholder}",
            (self.id,),
        )
        self.cursor.execute(
            "SELECT sequence_id FROM data_sync_revisions"
            f" WHERE datastore_id={self.placeholder}",
            (self.id,),
        )
        new_val = self.cursor.fetchone()[0]
        super()._increment_sequence_id()
        assert (
            self._sequence_id == new_val
        ), f"seq_id {self._sequence_id} DB seq_id {new_val}"

        return new_val

    def _put(self, doc: Document) -> None:
        """Put doc under docid.

        If no seq, give it one.
        """
        assert _REV in doc

        # "ON CONFLICT" added to sqlite upsert in version 3.24.0 (2018-06-04)
        # "ON CONFLICT" requires Postgres 9.5+
        set_statement = ", ".join(f"{col}=EXCLUDED.{col} " for col in self.columnnames)
        col_names = ",".join(self.columnnames)
        values = ",".join([self.placeholder for _ in self.columnnames])
        upsert_statement = (
            f"INSERT INTO {self.tablename} ({col_names}) VALUES ({values})"
            f" ON CONFLICT (_id) DO UPDATE"
            f" SET {set_statement}"
        )

        self.cursor.execute(
            upsert_statement, tuple(doc.get(key, None) for key in self.columnnames)
        )


class VersionError(Exception):
    pass


class SqliteDatastore(DatabaseDatastore):
    """Sqlite datastore."""

    def __init__(
        self, datastore_name: str, conn, tablename: str, datastore_id: str = None
    ):
        super().__init__(datastore_name, conn, tablename, datastore_id)
        # check sqlite version
        if sqlite3.sqlite_version_info < (3, 24, 0):
            raise VersionError(
                f"sqlite version is {sqlite3.sqlite_version},"
                " must be at least 3.24.0"
            )

        # set up SQL vars
        self.placeholder = "?"

    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        """Return doc, or None if not present."""
        doc = None
        # TODO: Use include_deleted in the query
        self.cursor.execute(f"SELECT * FROM {self.tablename} WHERE _id=?", (docid,))
        docrow = self.cursor.fetchone()
        if docrow:
            doc = self._row_to_doc(docrow)
            # assert there was only one result
            assert self.cursor.fetchone() is None, f"docid was {docid}"
            # Don't include deleted doc
            if doc.get(_DELETED, False) and not include_deleted:
                doc = None
        return doc

    def get_docs_since(self, the_seq: int, num: int) -> tuple[int, Sequence[Document]]:
        """Get docs put with the_seq < seq <= (the_seq+num), ordered by seq.

        This is intended to be called repeatedly to get them all, so as to
        allow syncing in chunks.
        """
        self.cursor.execute(
            f"SELECT * FROM {self.tablename}"
            " WHERE ? < _seq AND _seq <= ?"
            " ORDER BY _seq",
            (the_seq, the_seq + num),
        )
        docs = [self._row_to_doc(docrow) for docrow in self.cursor.fetchall()]
        return self.sequence_id, docs


class PostgresDatastore(DatabaseDatastore):
    def __init__(
        self, datastore_name: str, conn, tablename: str, datastore_id: str = None
    ):
        super().__init__(datastore_name, conn, tablename, datastore_id)
        self.placeholder = "%s"

    # def _set_sequence_id(self, the_id) -> None:
    #     # The RETURNING syntax has been supported by Postgres at least
    #     # since 9.5.
    #     # TODO: test setting different sequence ids for different datastores
    #     self.cursor.execute(
    #         "UPDATE data_sync_revisions set sequence_id = %s"
    #         " WHERE datastore_id = %s"
    #         " RETURNING sequence_id",
    #         (the_id, self.id),
    #     )
    #     new_val = self.cursor.fetchone()[0]
    #     self._sequence_id = the_id
    #     assert (
    #         self._sequence_id == new_val
    #     ), f"seq_id {self._sequence_id} DB seq_id {new_val}"

    def _increment_sequence_id(self) -> int:
        self.cursor.execute(
            "UPDATE data_sync_revisions set sequence_id = sequence_id+1"
            " WHERE datastore_id=%s"
            " RETURNING sequence_id",
            (self.id,),
        )
        new_val = self.cursor.fetchone()[0]
        self._sequence_id += 1
        assert (
            self._sequence_id == new_val
        ), f"seq_id {self._sequence_id} DB seq_id {new_val}"
        return new_val

    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        """Return doc, or None if not present."""
        doc = None
        # TODO: Use include_deleted in the query
        self.cursor.execute(f"SELECT * FROM {self.tablename} WHERE _id=%s", (docid,))

        docrow = self.cursor.fetchone()
        if docrow:
            doc = self._row_to_doc(docrow)
            # assert there was only one result
            assert self.cursor.fetchone() is None, f"docid was {docid}"
            # Don't include deleted doc
            if doc.get(_DELETED, False) and not include_deleted:
                doc = None
        return doc

    def get_docs_since(self, the_seq: int, num: int) -> tuple[int, Sequence[Document]]:
        """Get docs put with the_seq < seq <= (the_seq+num).

        This is intended to be called repeatedly to get them all, so as to
        allow syncing in chunks.
        """
        self.cursor.execute(
            f"SELECT * FROM {self.tablename} "
            "WHERE %s < _seq AND _seq <= %s ORDER BY _seq",
            (the_seq, the_seq + num),
        )
        docs = [self._row_to_doc(docrow) for docrow in self.cursor.fetchall()]
        return self.sequence_id, docs


class RestClientSourceDatastore(Datastore):
    """Communicate to a REST server for a datastore."""

    def __init__(self, baseurl: str, datastore_name: str):
        """Init a datastore.

        :param baseurl: The base URL of the REST server
        :param datastore_name:  Human-readable name
        """
        super().__init__(datastore_name)
        self.datastore_name = datastore_name
        self.baseurl = baseurl

    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        resp = requests.get(
            self._server_url(self.datastore_name + "/doc/" + docid),
            params={"include_deleted": include_deleted},
        )
        ret = None
        if resp.status_code == 200:
            ret = resp.json()
        return ret

    def _put(self, doc: Document):
        # We re-implemented put(), so we don't need _put()
        raise NotImplementedError("Not implemented")

    def put(self, doc: Document, increment_rev=False) -> tuple[int, Document]:
        logger.debug(
            f"RCSD {self.datastore_name}: put doc {doc}"
            f" increment_rev {increment_rev}"
        )
        resp = requests.post(
            self._server_url(self.datastore_name + "/doc"),
            params={"increment_rev": increment_rev},
            json=doc,
        )
        assert resp.status_code == 200, resp.status_code
        json = resp.json()
        return json["num_docs_put"], json["document"]

    # TODO: Unit test that deleted docs are included
    def get_docs_since(self, the_seq: int, num: int) -> tuple[int, Sequence[Document]]:
        the_url = self._server_url(self.datastore_name + "/docs")
        resp = requests.get(
            the_url,
            params={"start_sequence_id": the_seq, "chunk_size": num},
        )
        ret = None
        # TODO: What about 500?
        if resp.status_code == 200:
            js = resp.json()
            ret = (
                js["current_sequence_id"],
                [Document(doc) for doc in js["documents"]],
            )
        elif resp.status_code == 404:
            raise ValueError(f"{the_url} returned a 404 (not found)")
        return ret

    def _server_url(self, url: str) -> str:
        return self.baseurl + url
