import logging

from .datastore import _REV


def sync_pull(client, server):
    # client sync: get objects from server
    for doc in server.get_docs_since(
            client.get_peer_sequence_id(server.id)):
        seq = doc[_REV]
        client.put_if_needed(doc)
        client.set_peer_sequence_id(server.id, seq)


def sync_both(client, server):
    # client sync: get objects from server
    logging.info("*************** sync from server to client")
    sync_pull(client, server)
    # client sync: put its objects in server
    logging.info("*************** sync from client to server")
    sync_pull(server, client)
