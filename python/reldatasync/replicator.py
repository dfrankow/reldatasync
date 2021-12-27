import logging

from reldatasync.datastore import Datastore

logger = logging.getLogger(__name__)


class Replicator:
    def __init__(self, source: Datastore,
                 destination: Datastore,
                 chunk_size: int = 10):
        self.source = source
        self.destination = destination
        self.chunk_size = chunk_size

    @staticmethod
    def _pull_changes(destination, source, chunk_size) -> int:
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
            #      destination.sequence_id >= max([doc[_SEQ] for doc in docs]))

            # If we got all docs to (new_peer_seq_id+chunk_size), then either
            # we stepped forward to that, or to the latest the source had
            new_peer_seq_id = min(source_seq_id, new_peer_seq_id+chunk_size)

        # source_seq_id is at least as new as the docs that came over
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

    def _push_changes(self) -> int:
        """Push changes from destination to source."""
        return Replicator._pull_changes(
            self.destination, self.source, self.chunk_size)

    def pull_changes(self) -> int:
        """Pull changes from source to destination.

        Also update self seq id, and self peer seq id.

        :param chunk_size  Approximate number of docs per chunk

        :return: number of docs changed (in self).
        """
        return Replicator._pull_changes(
            self.source, self.destination, self.chunk_size)

    def sync_both_directions(self) -> None:
        """Sync client and server in both directions

        Completely updated by the end, unless destination has been changing.

        :param self: one datastore
        :return: None
        """
        # Here is an example diagram, with source on the left, dest on the
        # right source made 2 changes, dest made 3, now they are going to sync.
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
            f'******* push changes from {self.source.id}'
            f' to {self.destination.id}')
        self._push_changes()

        # 2. destination -> source
        logger.debug(
            f'******* pull changes from {self.destination.id}'
            f' to {self.source.id}')
        self.pull_changes()

        # 3. push source seq -> destination seq
        logger.debug(
            f'******* push changes from {self.source.id}'
            f' to {self.destination.id}')
        # source.set_peer_sequence_id(destination.id, destination.sequence_id)
        final_changes = self._push_changes()

        # Since nothing else changed, only the sequence # was synchronized
        assert final_changes == 0, f'actually had {final_changes} changes'

        # This is no longer true.  Their clocks may not be synchronized if
        # changes are ignored (and the sequence_id doesn't go up).
        # now their "clocks" are synchronized
        # assert destination.sequence_id == self.sequence_id, (
        #     "server.sequence_id %d client.sequence_id %s" %
        #     (destination.sequence_id, self.sequence_id))

        # ### debug logging
        # logger.debug(f'{self.id} seq {self._sequence_id}')
        # logger.debug(f'{destination.id} seq {destination._sequence_id}')
        # for peer in self.peer_seq_ids:
        #     logger.debug(f'{self.id} peer_seq_ids[{peer}]'
        #                  f'={self.peer_seq_ids[peer]}')
        # for peer in destination.peer_seq_ids:
        #     logger.debug(f'{destination.id} peer_seq_ids[{peer}]'
        #                  f'={destination.peer_seq_ids[peer]}')
        # ###

        # now they know about each others' clocks
        assert (self.destination.get_peer_sequence_id(self.source.id)
                == self.source.sequence_id), (
            f'{self.destination.id} thinks {self.source.id} seq is '
            f'{self.destination.get_peer_sequence_id(self.source.id)}, '
            f'{self.source.id} thinks seq is {self.source.sequence_id}')

        # TODO: decide if this assert is still right
        # assert (self.get_peer_sequence_id(destination.id)
        #         == destination.sequence_id), (
        #     f'{self.id} thinks {destination.id} seq is '
        #     f'{self.get_peer_sequence_id(destination.id)},'
        #     f' {destination.id} thinks seq is {destination.sequence_id}')

        logger.debug(
            f'******** sync done, {self.source.id} seq'
            f' is {self.source.sequence_id},'
            f' {self.destination.id} seq is {self.destination.sequence_id}')
