"""An abstraction of a datastore, to use for syncing."""

from abc import ABC, abstractmethod

import logging
from typing import Sequence, TypeVar, Generic

import psycopg2

ID = TypeVar('ID')

_REV = '_rev'
_ID = '_id'
_DELETED = '_deleted'

logger = logging.getLogger(__name__)


class Document(dict):
    def __init__(self, *arg, **kw):
        super(Document, self).__init__(*arg, **kw)
        assert '_id' in self

    @staticmethod
    def _compare_vals(one, two):
        # comparisons have to happen in the right order to respect None
        if one is None and two is None:
            return 0
        elif one is None and two is not None:
            return -1
        elif one is not None and two is None:
            return 1
        elif one < two:
            return -1
        elif one > two:
            return 1
        else:
            return 0

    def _compare(self, other):
        """Return -1 if doc1 < doc2, 0 if equal, 1 if doc1 > doc2"""
        # compare keys
        if len(self) < len(other):
            return -1
        elif len(self) > len(other):
            return 1
        else:
            # same number of keys, now compare them
            keys1 = sorted(self.keys())
            keys2 = sorted(other.keys())
            for idx in range(len(keys1)):
                keycmp = Document._compare_vals(keys1[idx], keys2[idx])
                if keycmp != 0:
                    return keycmp

            # keys were all the same, now compare values
            for idx in range(len(self)):
                valcmp = Document._compare_vals(
                    self[keys1[idx]], other[keys2[idx]])
                if valcmp != 0:
                    return valcmp

            # everything was equal
            return 0

    def __eq__(self, other):
        return self._compare(other) == 0

    def __ne__(self, other):
        return self._compare(other) != 0

    def __lt__(self, other):
        return self._compare(other) < 0

    def __le__(self, other):
        return self._compare(other) != 1

    def __gt__(self, other):
        return self._compare(other) > 0

    def __ge__(self, other):
        return self._compare(other) != -1

    def copy(self):
        return Document(super().copy())


class Datastore(Generic[ID], ABC):
    def __init__(self, datastore_id: str):
        self.id = datastore_id
        self._sequence_id = 0
        self.peer_seq_ids = {}

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def _increment_sequence_id(self):
        self._sequence_id += 1

    def _set_sequence_id(self, the_id):
        logger.info("%s: set seq_id from %d to %d"
                    % (self.id, self.sequence_id, the_id))
        assert the_id >= self._sequence_id
        self._sequence_id = the_id

    @property
    def sequence_id(self):
        """Read-only sequence_id"""
        return self._sequence_id

    def _pre_put(self, doc):
        # copy doc so we don't modify caller's doc
        doc = doc.copy()

        if doc.get(_REV, None) is None:
            self._increment_sequence_id()
            doc[_REV] = self._sequence_id

        logger.debug("datastore %s put docid %s seq %s doc %s" % (
            self.id, doc[_ID], doc[_REV], doc
        ))

        return doc

    def put_if_needed(self, doc: Document) -> None:
        """Put doc under docid if seq is greater"""
        docid = doc[_ID]
        seq = doc[_REV]
        my_doc = self.get(docid)
        my_seq = my_doc.get(_REV, None) if my_doc else None
        if (my_seq is None) or (my_seq < seq) or (my_doc < doc):
            self.put(doc)
        else:
            logger.debug("Ignore docid %s doc %s seq %s "
                         "(compared to doc %s seq %s)" % (
                          docid, doc, seq, my_doc, my_seq))

    def delete(self, docid: ID) -> None:
        """Delete an doc in the datastore.

        Returns silently if the doc is not in the datastore.
        """
        doc = self.get(docid)
        if doc and not doc.get(_DELETED, False):
            doc[_DELETED] = True
            # Deletion makes a new rev
            self._increment_sequence_id()
            doc[_REV] = self._sequence_id
            self.put(doc)
            logger.debug("deleted doc: %s" % doc)

    def get_peer_sequence_id(self, peer: str):
        """Get the seq we have for peer, or zero if we have none."""
        return self.peer_seq_ids.get(peer, 0)

    def set_peer_sequence_id(self, peer: str, seq: int):
        """Set new peer sequence id, if seq > what we have."""
        if seq > self.get_peer_sequence_id(peer):
            self.peer_seq_ids[peer] = seq
            action = "set"
        else:
            action = "ignore setting"

        logger.info("%s: %s %s seq_id from %d to %d"
                    % (self.id, action, peer,
                       self.get_peer_sequence_id(peer), seq))

    @abstractmethod
    def get(self, docid: ID) -> Document:
        pass

    @abstractmethod
    def put(self, doc: Document) -> None:
        pass

    @abstractmethod
    def get_docs_since(self, the_seq: int) -> Sequence[Document]:
        pass


class MemoryDatastore(Datastore):
    """An in-memory transient datastore, only useful for testing."""
    def __init__(self, datastore_id: str):
        super().__init__(datastore_id)
        self.datastore = {}

    def get(self, docid: ID) -> Document:
        """Return doc, or None if not present."""
        doc = self.datastore.get(docid, None)
        # Return a copy so our internals cannot be modified
        if doc:
            doc = doc.copy()
        return doc

    def put(self, doc: Document) -> None:
        """Put doc under docid.

        If no seq, give it one.
        """
        doc = self._pre_put(doc)
        self.datastore[doc[_ID]] = doc

    def get_docs_since(self, the_seq: int) -> Sequence[Document]:
        """Get docs put with seq > the_seq, unordered."""
        for docid, doc in self.datastore.items():
            if doc[_REV] > the_seq:
                yield doc


class PostgresDatastore(Datastore):
    def __init__(self, datastore_id: str, conn_str: str,
                 tablename: str):
        super().__init__('%s.%s' % (datastore_id, tablename))
        self.tablename = tablename
        self.conn_str = conn_str

    def __enter__(self):
        super().__enter__()
        self.conn = psycopg2.connect(self.conn_str)

        # Autocommit to allow two connections to the same DB without deadlock
        # TODO(dan): Should any class members not auto-commit?
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.cursor = self.conn.cursor()

        # TODO(dan): Create the data_sync_revisions table if needed

        # Init sequence_id if not present
        # "ON CONFLICT" requires postgres 9.5+
        # See also https://stackoverflow.com/a/17267423/34935
        # See also https://stackoverflow.com/a/30118648/34935
        self.cursor.execute(
            "INSERT INTO data_sync_revisions (datastore_id, sequence_id)"
            " VALUES (%s, 0)"
            " ON CONFLICT DO NOTHING", (self.id,))

        # Check that the right tables exist
        self.cursor.execute("SELECT sequence_id FROM data_sync_revisions")
        self._sequence_id = self.cursor.fetchone()[0]

        # Get the column names for self.tablename
        self.cursor.execute("SELECT * FROM %s LIMIT 0" % self.tablename)
        self.columnnames = [desc[0] for desc in self.cursor.description]
        self.nonid_columnnames = [name for name in self.columnnames
                                  if name != _ID]

        # Check that self.tablename has _id, _deleted, and _rev
        for field in (_ID, _REV, _DELETED):
            if field not in self.columnnames:
                raise NameError("Field '%s' not in table '%s'" % (
                    field, self.tablename))

        # TODO(dan): Check self.tablename has a unique index on _id
        # Required for proper functioning of Postgres UPSERT
        # See also https://stackoverflow.com/a/36799500/34935

        return self

    def __exit__(self, *args):
        super().__exit__()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _row_to_doc(self, docrow):
        the_dict = {}
        assert len(docrow) == len(self.columnnames)
        for idx in range(len(docrow)):
            the_dict[self.columnnames[idx]] = docrow[idx]
        # Treat '_deleted' specially: get rid of it unless True
        if not the_dict[_DELETED]:
            del the_dict[_DELETED]
        return Document(the_dict)

    def _set_sequence_id(self, the_id):
        # Get this to work:
        # self.cursor.execute(
        #     "UPDATE data_sync_revisions set sequence_id = %s"
        #     " RETURNING sequence_id", the_id)
        self.cursor.execute(
            "UPDATE data_sync_revisions set sequence_id = %s"
            " RETURNING sequence_id" % the_id)
        new_val = self.cursor.fetchone()[0]
        super()._set_sequence_id(the_id)
        assert self._sequence_id == new_val, (
                'seq_id %d DB seq_id %d' % (self._sequence_id, new_val))

    def get(self, docid: ID) -> Document:
        """Return doc, or None if not present."""
        doc = None
        self.cursor.execute(
            "SELECT * FROM %s WHERE _id=%%s" % self.tablename, docid)
        docrow = self.cursor.fetchone()
        if docrow:
            doc = self._row_to_doc(docrow)
            # assert there was only one result
            assert self.cursor.fetchone() is None, 'docid was %s' % docid
        return doc

    def put(self, doc: Document) -> None:
        """Put doc under docid.

        If no seq, give it one.
        """
        doc = self._pre_put(doc)

        # "ON CONFLICT" requires postgres 9.5+
        set_statement = ', '.join("%s=EXCLUDED.%s " % (col, col)
                                  for col in self.columnnames)
        upsert_statement = (
            "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (_id) DO UPDATE"
            " SET %s" % (
                self.tablename,
                ','.join(self.columnnames),
                ','.join([r'%s' for _ in self.columnnames]),
                set_statement))

        self.cursor.execute(
            upsert_statement,
            tuple([doc.get(key, None) for key in self.columnnames]))

    def get_docs_since(self, the_seq: int) -> Sequence[Document]:
        """Get docs put with seq > the_seq, unordered."""
        self.cursor.execute(
            "SELECT * FROM %s WHERE _rev > %%s" % self.tablename, (the_seq,))
        for docrow in self.cursor.fetchall():
            yield self._row_to_doc(docrow)

    def _increment_sequence_id(self):
        self.cursor.execute(
            "UPDATE data_sync_revisions set sequence_id = sequence_id+1"
            " WHERE datastore_id=%s"
            " RETURNING sequence_id", (self.id,))
        new_val = self.cursor.fetchone()[0]
        super()._increment_sequence_id()
        assert self._sequence_id == new_val, (
                'seq_id %d DB seq_id %d' % (self._sequence_id, new_val))
