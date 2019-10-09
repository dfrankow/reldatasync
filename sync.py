import logging

from .datastore import _REV


def sync_pull(client, server):
    # client sync: get docs from server with lowest seqs since we last synced
    for doc in server.get_docs_since(
            client.get_peer_sequence_id(server.id)):
        seq = doc[_REV]
        client.put_if_needed(doc)

    # client has all updates up to server.sequence_id now
    client.set_peer_sequence_id(server.id, server.sequence_id)

    # keep client sequence_id up to date
    # in other words, catch up my clock to the other clock
    # otherwise, my revs could start losing all the time
    if client.sequence_id < server.sequence_id:
        client.sequence_id = server.sequence_id


def sync_both(client, server):
    # client sync: get objects from server
    logging.info("*************** sync from server to client")
    sync_pull(client, server)
    # client sync: put its objects in server
    logging.info("*************** sync from client to server")
    sync_pull(server, client)

    # now their "clocks" are synchronized
    assert server.sequence_id == client.sequence_id, (
        "server.sequence_id %d client.sequence_id %s" %
        (server.sequence_id, client.sequence_id))
    # now they know about each others' clocks
    assert server.get_peer_sequence_id(client.id) == client.sequence_id, (
        'server thinks client seq is %d, client thinks seq is %d' % (
         server.get_peer_sequence_id(client.id), client.sequence_id))
    assert client.get_peer_sequence_id(server.id) == server.sequence_id
