import logging

logger = logging.getLogger(__name__)


def sync_pull(destination, source):
    """Pull changes from source to destination.

    Also update destination seq id, and destination peer seq id.
    """
    # destination sync: get docs from source with lowest seqs
    # since we last synced
    old_peer_seq_id = destination.get_peer_sequence_id(source.id)
    for doc in source.get_docs_since(old_peer_seq_id):
        destination.put_if_needed(doc)

    # destination has all updates up to source.sequence_id now
    assert source.sequence_id >= old_peer_seq_id
    destination.set_peer_sequence_id(source.id, source.sequence_id)

    # keep destination sequence_id up to date
    # in other words, catch up my clock to the other clock
    # otherwise, my revs could start losing all the time
    if destination.sequence_id < source.sequence_id:
        destination._set_sequence_id(source.sequence_id)


def sync_both(client, server):
    """Sync client and server in both directions, completely updated by the end.

    :param client: one datastore
    :param server: another datastore
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

    # 1. server -> client sync: get objects from server
    logger.info("*************** sync from server to client")
    sync_pull(client, server)
    # 2. client -> server sync: put client objects in server
    logger.info("*************** sync from client to server")
    sync_pull(server, client)
    # 3. sync server seq -> client's server seq
    client.set_peer_sequence_id(server.id, server.sequence_id)

    # now their "clocks" are synchronized
    assert server.sequence_id == client.sequence_id, (
        "server.sequence_id %d client.sequence_id %s" %
        (server.sequence_id, client.sequence_id))
    # now they know about each others' clocks
    assert server.get_peer_sequence_id(client.id) == client.sequence_id, (
        'server thinks client seq is %d, client thinks seq is %d' % (
         server.get_peer_sequence_id(client.id), client.sequence_id))
    assert client.get_peer_sequence_id(server.id) == server.sequence_id, (
        'client thinks server seq is %d, server thinks seq is %d' % (
         client.get_peer_sequence_id(server.id), server.sequence_id))

    logger.info("*************** sync done, seq is %d" % client.sequence_id)
