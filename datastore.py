"""An abstraction of a datastore, to use for syncing."""

import logging
from typing import Any


class MemoryDatastore():
    def __init__(self, ds_id):
        self.id = ds_id
        self.datastore = {}
        self.sequence_id = 0
        self.peer_seq_ids = {}

    def get(self, key) -> (Any, Any):
        """Return object and version, or (None, None) if not present."""
        return self.datastore.get(key, (None, None))

    def put(self, key, obj, seq=None):
        """Put obj under key, optionally with seq.

        If no seq, give it one.
        """
        if seq is None:
            self.sequence_id += 1
            seq = self.sequence_id
        self.datastore[key] = obj, seq
        logging.info("datastore %s put key %s obj %s seq %s" % (
            self.id, key, obj, self.sequence_id
        ))

    def put_if_needed(self, key, obj, seq):
        """Put obj under key if seq is greater"""
        my_obj, my_seq = self.get(key)
        if (my_seq is None) or (my_seq < seq) or (my_obj < obj):
            self.put(key, obj, seq)
        else:
            logging.info("Ignore key %s obj %s seq %s "
                         "(compared to obj %s seq %s)" % (
                key, obj, seq, my_obj, my_seq))

    def get_objects_since(self, the_seq):
        """Get objects put with seq > the_seq

        Must be returned in order.
        """
        for key, obj_tuple in self.datastore.items():
            obj, sequence_id = obj_tuple
            if sequence_id > the_seq:
                yield key, obj, sequence_id

    def get_peer_sequence_id(self, peer):
        """Get the seq we have for peer, or zero if we have none."""
        return self.peer_seq_ids.get(peer, 0)

    def set_peer_sequence_id(self, peer, seq):
        """Get the seq we have for peer"""
#        assert seq >= self.get_peer_sequence_id(peer), (
#            'seq %s peer seq %s' % (seq, self.get_peer_sequence_id(peer)))
        self.peer_seq_ids[peer] = seq
