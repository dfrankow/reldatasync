"""An abstraction of a datastore, to use for syncing."""

import logging
from typing import Sequence, TypeVar, Generic

ID = TypeVar('ID')

_REV = '_rev'
_ID = '_id'
_DELETED = '_deleted'


class Document(dict):
    def __init__(self, *arg, **kw):
        super(Document, self).__init__(*arg, **kw)
        assert '_id' in self

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
                if keys1[idx] < keys2[idx]:
                    return -1
                elif keys1[idx] > keys2[idx]:
                    return 1

            # keys were all the same, now compare values
            for idx in range(len(self)):
                if self[keys1[idx]] < other[keys2[idx]]:
                    return -1
                elif self[keys1[idx]] > other[keys2[idx]]:
                    return 1

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


class MemoryDatastore(Generic[ID]):
    """The memory datastore is transient.  It's only useful for testing."""
    def __init__(self, datastore_id: str):
        self.id = datastore_id
        self.datastore = {}
        self.sequence_id = 0
        self.peer_seq_ids = {}

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
        # copy doc so we don't modify caller's doc
        doc = doc.copy()

        if doc.get(_REV, None) is None:
            self.sequence_id += 1
            doc[_REV] = self.sequence_id

        docid = doc[_ID]
        self.datastore[docid] = doc
        logging.info("datastore %s put docid %s seq %s doc %s" % (
            self.id, docid, doc[_REV], doc
        ))

    def put_if_needed(self, doc: Document) -> None:
        """Put doc under docid if seq is greater"""
        docid = doc[_ID]
        seq = doc[_REV]
        my_doc = self.get(docid)
        my_seq = my_doc.get(_REV, None) if my_doc else None
        if (my_seq is None) or (my_seq < seq) or (my_doc < doc):
            self.put(doc)
        else:
            logging.info("Ignore docid %s doc %s seq %s "
                         "(compared to doc %s seq %s)" % (
                          docid, doc, seq, my_doc, my_seq))

    def get_docs_since(self, the_seq: int) -> Sequence[Document]:
        """Get docs put with seq > the_seq
        """
        result = []
        for docid, doc in self.datastore.items():
            if doc[_REV] > the_seq:
                result.append(doc)
        return result

    def get_peer_sequence_id(self, peer: str):
        """Get the seq we have for peer, or zero if we have none."""
        return self.peer_seq_ids.get(peer, 0)

    def set_peer_sequence_id(self, peer: str, seq: int):
        """Get the seq we have for peer"""
#        assert seq >= self.get_peer_sequence_id(peer), (
#            'seq %s peer seq %s' % (seq, self.get_peer_sequence_id(peer)))
        self.peer_seq_ids[peer] = seq
