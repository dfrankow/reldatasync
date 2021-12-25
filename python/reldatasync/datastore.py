"""An abstraction of a datastore, to use for syncing."""

from abc import ABC, abstractmethod
from collections import OrderedDict
import logging
import psycopg2

from typing import Sequence, Generic, Tuple

from reldatasync.document import Document, _REV, _ID, _DELETED, ID_TYPE, _SEQ
from reldatasync.vectorclock import VectorClock

logger = logging.getLogger(__name__)


class Datastore(Generic[ID_TYPE], ABC):
    def __init__(self, datastore_id: str):
        self.id = datastore_id
        self._sequence_id = 0
        self.peer_seq_ids = {}

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def _increment_sequence_id(self) -> int:
        self._sequence_id += 1
        logger.debug(
            f"{self.id}: Increment self._sequence_id to {self._sequence_id}")
        return self._sequence_id

    def _set_sequence_id(self, the_id) -> None:
        assert the_id >= self._sequence_id
        self._sequence_id = the_id

    @property
    def sequence_id(self) -> int:
        """Read-only sequence_id"""
        return self._sequence_id

    def _set_new_rev(self, doc: Document, seq_id: int) -> None:
        """Set increment_rev revision for a doc.

        Return new sequence id."""
        rev = VectorClock.from_string(doc.get(_REV, "{}"))
        rev.set_clock(self.id, seq_id)
        doc[_REV] = str(rev)

    def put(self, doc: Document, increment_rev=False) -> int:
        """Put doc under docid if rev is greater, or doc doesn't currently exist

        Return number of records actually put (0 or 1).

        As a side effect, this updates self.sequence_id if doc is put.

        :param doc  Document to put
        :param increment_rev  If True, increment revision.  If revision is
                              not present, it adds one.
        """
        if not increment_rev and _REV not in doc:
            raise ValueError(f"doc must have {_REV} if increment_rev is False")

        # copy doc so we don't modify caller's doc
        doc = doc.copy()

        ret = 0
        docid = doc[_ID]

        rev_str = doc.get(_REV, None)
        if increment_rev:
            if rev_str is None:
                rev_str = '{}'
            rev = VectorClock.from_string(rev_str)
            # the new rev below would be >= to this:
            rev.set_clock(self.id, self.sequence_id+1)
        else:
            # we threw a ValueError above if it was None
            assert rev_str is not None
            rev = VectorClock.from_string(rev_str)

        my_doc = self.get(docid, include_deleted=True)

        my_rev = VectorClock.from_string(my_doc.get(_REV)) if my_doc else None
        if (my_rev is None) or (my_rev < rev):
            seq_id = self._increment_sequence_id()
            if increment_rev:
                # Now assign the rev for real
                # Maybe sequence id changed since we looked above, get the real one
                rev.set_clock(self.id, seq_id)
                doc[_REV] = str(rev)
            doc[_SEQ] = seq_id
            self._put(doc)
            ret = 1

            logger.debug(
                f"Put docid {docid} doc {doc} seq {rev} "
                f"in {self.id} (compared to doc {my_doc} seq {my_rev})")
        else:
            logger.debug("Ignore docid %s doc %s seq %s "
                         "(compared to doc %s seq %s)" % (
                          docid, doc, rev, my_doc, my_rev))
        return ret

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
            self._put(doc)

    def get_peer_sequence_id(self, peer: str) -> int:
        """Get the seq we have for peer, or zero if we have none."""
        return self.peer_seq_ids.get(peer, 0)

    def set_peer_sequence_id(self, peer: str, seq: int) -> None:
        """Set increment_rev peer sequence id, if seq > what we have."""
        if seq > self.get_peer_sequence_id(peer):
            self.peer_seq_ids[peer] = seq

    @abstractmethod
    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        pass

    @abstractmethod
    def _put(self, doc: Document) -> None:
        pass

    @abstractmethod
    def get_docs_since(self, the_seq: int, num: int) \
            -> Tuple[int, Sequence[Document]]:
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
        pass

    @staticmethod
    def _pull_changes(destination, source, chunk_size=10) -> int:
        """Pull changes from source to destination.

        Also update destination seq id, and destination peer seq id.

        :param destination  Where changes end up
        :param source  Where changes come from
        :param chunk_size Approximate chunk size to use during operation

        :return: number of docs changed on destination
        """
        # destination sync: get docs from source with lowest seqs
        # since we last synced
        docs_changed = 0
        old_peer_seq_id = destination.get_peer_sequence_id(source.id)
        new_peer_seq_id = old_peer_seq_id
        # get docs in chunks of approximately chunk_size
        source_seq_id = None
        # Move forward in chunks of chunk_size, but only to source_seq_id
        while source_seq_id is None or source_seq_id > new_peer_seq_id:
            source_seq_id, docs = source.get_docs_since(
                new_peer_seq_id, chunk_size)
            for doc in docs:
                docs_changed += destination.put(doc)

            # This used to be true, but now it's not.  If the destination
            # ignores some things, then its sequence_id may not rise.
            #
            # "destination seq_id is at least as big as the docs we put in"
            # assert (len(docs) == 0 or
            #         destination.sequence_id >= max([doc[_SEQ] for doc in docs]))

            # If we got all docs to (new_peer_seq_id+chunk_size), then either
            # we stepped forward to that, or to the latest the source had
            new_peer_seq_id = min(source_seq_id, new_peer_seq_id+chunk_size)

        # source_seq_id is at least as increment_rev as the docs that came over
        assert source_seq_id >= new_peer_seq_id, (
            'source seq %d increment_rev peer seq %d' % (
             source.sequence_id, new_peer_seq_id))

        # we moved forward, or there were no updates
        assert (new_peer_seq_id > old_peer_seq_id
                or source_seq_id == old_peer_seq_id)
        assert (new_peer_seq_id > old_peer_seq_id or docs_changed == 0)

        # we've got up to new_peer_seq_id, so dest must be >= that
        destination.set_peer_sequence_id(source.id, new_peer_seq_id)

        return docs_changed

    def push_changes(self, destination, chunk_size=10) -> int:
        """Push changes from self to destination.

        Also update destination seq id, and destination peer seq id.

        :param destination  Where changes end up
        :param chunk_size  Approximate number of docs per chunk
        :return: number of docs changed on destination.
        """
        return Datastore._pull_changes(destination, self, chunk_size=chunk_size)

    def pull_changes(self, source, chunk_size=10) -> int:
        """Pull changes from source to self.

        Also update self seq id, and self peer seq id.

        :param source  Where changes come from
        :param chunk_size  Approximate number of docs per chunk

        :return: number of docs changed (in self).
        """
        return Datastore._pull_changes(self, source, chunk_size=chunk_size)

    def sync_both_directions(self, destination, chunk_size=10) -> None:
        """Sync client and server in both directions

        Completely updated by the end, unless destination has been changing.

        :param self: one datastore
        :param destination: another datastore
        :param chunk_size: Approx. chunk_size to use for each directional sync
        :return: None
        """
        # Here is an example diagram, with source on the left, dest on the right
        # source made 2 changes, dest made 3, now they are going to sync.
        #
        #   source: 2
        #   dest  : 0
        #
        #                   dest  : 3
        #                   source: 0
        #
        #   source: 2 ----> dest  : 3*
        #   dest  : 0       source: 2*
        #
        #   source: 3* <--- dest  : 3*
        #   dest  : 3*      source: 2
        #
        #   source: 3* ---> dest  : 3*
        #   dest  : 3*      source: 3*

        # 1. source -> destination
        logger.debug(
            f"******* push changes from {self.id} to {destination.id}")
        self.push_changes(destination, chunk_size=chunk_size)
        # 2. destination -> source
        logger.debug(
            f"******* pull changes from {destination.id} to {self.id}")
        self.pull_changes(destination, chunk_size=chunk_size)
        # 3. push source seq -> destination seq
        logger.debug(
            f"******* push changes from {self.id} to {destination.id} 2")
        # source.set_peer_sequence_id(destination.id, destination.sequence_id)
        final_changes = self.push_changes(destination, chunk_size=chunk_size)

        # Since nothing else changed, only the sequence # was synchronized
        assert final_changes == 0, 'actually had %d changes' % final_changes

        # This is no longer true.  Their clocks may not be synchronized if
        # changes are ignored (and the sequence_id doesn't go up).
        # now their "clocks" are synchronized
        # assert destination.sequence_id == self.sequence_id, (
        #     "server.sequence_id %d client.sequence_id %s" %
        #     (destination.sequence_id, self.sequence_id))

        # now they know about each others' clocks
        assert destination.get_peer_sequence_id(self.id) == self.sequence_id, (
            'server thinks client seq is %d, client thinks seq is %d' % (
             destination.get_peer_sequence_id(self.id), self.sequence_id))
        assert (self.get_peer_sequence_id(destination.id)
                == destination.sequence_id), (
            'client thinks server seq is %d, server thinks seq is %d' % (
             self.get_peer_sequence_id(destination.id), destination.sequence_id))

        logger.debug("************ sync done, seq is %d" % self.sequence_id)


class MemoryDatastore(Datastore):
    """An in-memory transient datastore, only useful for testing."""
    def __init__(self, datastore_id: str):
        super().__init__(datastore_id)
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
            if doc.get(_DELETED, False) and not include_deleted:
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

    def get_docs_since(self, the_seq: int, num: int) \
            -> Tuple[int, Sequence[Document]]:
        """Get docs put with the_seq < seq <= (the_seq+num).

        This is intended to be called repeatedly to get them all, so as to
        allow syncing in chunks.

        :return  current sequence id, docs
        """
        docs = []
        for docid, doc in self.datastore.items():
            doc_seq = doc[_SEQ]
            assert doc_seq is not None
            if the_seq < doc_seq <= (the_seq + num):
                docs.append(doc)
            # Since self.datastore is ordered, we can cut out early
            # This is optional, perhaps premature optimization
            if doc_seq > (the_seq + num):
                break
        return self.sequence_id, docs


class PostgresDatastore(Datastore):
    def __init__(self, datastore_id: str, conn_str: str,
                 tablename: str):
        super().__init__('%s.%s' % (datastore_id, tablename))
        self.tablename = tablename
        self.conn_str = conn_str

    def __enter__(self):
        super().__enter__()
        self.conn = psycopg2.connect(self.conn_str)

        # TODO(dan): What exactly is our commit policy?
        # self.conn.set_isolation_level(
        #     psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.cursor = self.conn.cursor()

        # TODO(dan): Create the data_sync_revisions table if needed

        # Init sequence_id if not present
        # "ON CONFLICT" requires postgres 9.5+
        # See also https://stackoverflow.com/a/17267423
        # See also https://stackoverflow.com/a/30118648
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
        # self.nonid_columnnames = [name for name in self.columnnames
        #                           if name != _ID]

        # Check that self.tablename has _id, _deleted, and _rev
        for field in (_ID, _REV, _DELETED):
            if field not in self.columnnames:
                raise NameError("Field '%s' not in table '%s'" % (
                    field, self.tablename))

        # TODO: Check self.tablename has a unique index on _id
        # Required for proper functioning of Postgres UPSERT
        # See also https://stackoverflow.com/a/36799500

        # TODO: Check that sequence_id in revisions table is >= max(REV)
        #   in the data

        return self

    def __exit__(self, *args):
        super().__exit__()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _row_to_doc(self, docrow) -> Document:
        the_dict = {}
        assert len(docrow) == len(self.columnnames)
        for idx in range(len(docrow)):
            the_dict[self.columnnames[idx]] = docrow[idx]
        # Treat '_deleted' specially: get rid of it unless True
        if not the_dict[_DELETED]:
            del the_dict[_DELETED]
        return Document(the_dict)

    def _set_sequence_id(self, the_id) -> None:
        self.cursor.execute(
            "UPDATE data_sync_revisions set sequence_id = %s"
            " RETURNING sequence_id", (the_id,))
        new_val = self.cursor.fetchone()[0]
        super()._set_sequence_id(the_id)
        assert self._sequence_id == new_val, (
                'seq_id %d DB seq_id %d' % (self._sequence_id, new_val))

    def get(self, docid: ID_TYPE, include_deleted=False) -> Document:
        """Return doc, or None if not present."""
        doc = None
        # TODO: Use include_deleted in the query
        self.cursor.execute(
            "SELECT * FROM %s WHERE _id=%%s" % self.tablename, (docid,))
        docrow = self.cursor.fetchone()
        if docrow:
            doc = self._row_to_doc(docrow)
            # assert there was only one result
            assert self.cursor.fetchone() is None, 'docid was %s' % docid
            # Don't include deleted doc
            if doc.get(_DELETED, False) and not include_deleted:
                doc = None
        return doc

    def _put(self, doc: Document) -> None:
        """Put doc under docid.

        If no seq, give it one.
        """
        assert _REV in doc

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

    def get_docs_since(self, the_seq: int, num: int) \
            -> Tuple[int, Sequence[Document]]:
        """Get docs put with the_seq < seq <= (the_seq+num).

        This is intended to be called repeatedly to get them all, so as to
        allow syncing in chunks.
        """
        self.cursor.execute(
            "SELECT * FROM %s WHERE %%s < _seq AND _seq <= %%s"
            % self.tablename, (the_seq, the_seq + num))
        docs = [self._row_to_doc(docrow) for docrow in self.cursor.fetchall()]
        return self.sequence_id, docs

    def _increment_sequence_id(self) -> int:
        self.cursor.execute(
            "UPDATE data_sync_revisions set sequence_id = sequence_id+1"
            " WHERE datastore_id=%s"
            " RETURNING sequence_id", (self.id,))
        new_val = self.cursor.fetchone()[0]
        super()._increment_sequence_id()
        assert self._sequence_id == new_val, (
                'seq_id %d DB seq_id %d' % (self._sequence_id, new_val))
        return new_val