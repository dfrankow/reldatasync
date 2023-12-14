#!/usr/bin/env python

"""
A simple test client that synchronizes one table and prints the output to stdout.
"""
import argparse
import json

from reldatasync import util
from reldatasync.datastore import MemoryDatastore, RestClientSourceDatastore
from reldatasync.document import _SEQ
from reldatasync.replicator import Replicator


def main():
    parser = argparse.ArgumentParser(description="Test REST server.")
    parser.add_argument("--server-url", "-s", required=True, help="URL of the server")
    parser.add_argument("--log-level", "-l", default="WARNING", help="Log level")
    parser.add_argument("--datastore", required=True, help="Datastore name")
    args = parser.parse_args()
    util.logging_basic_config(level=args.log_level)

    remote_ds = RestClientSourceDatastore(args.server_url, args.datastore)

    # Put docs in a local datastore
    ds = MemoryDatastore("client")
    Replicator(ds, remote_ds).sync_both_directions()

    # Write out the results
    done = False
    seq = 0
    while not done:
        # Note: assumes <= 1000000 docs
        _, docs = ds.get_docs_since(seq, 10)
        for doc in docs:
            seq = max(seq, doc[_SEQ])
            print(json.dumps(doc))

        if not docs:
            done = True


if __name__ == "__main__":
    main()
