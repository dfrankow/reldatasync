#!/usr/bin/env python

"""
A simple test client that synchronizes one table and prints the output to stdout.
"""

import argparse
import json
import sqlite3

from reldatasync import util
from reldatasync.datastore import (
    MemoryDatastore,
    RestClientSourceDatastore,
    SqliteDatastore,
)
from reldatasync.document import _SEQ
from reldatasync.replicator import Replicator


def main():
    parser = argparse.ArgumentParser(description="Test REST server.")
    parser.add_argument("--server-url", "-s", required=True, help="URL of the server")
    parser.add_argument("--log-level", "-l", default="WARNING", help="Log level")
    parser.add_argument("--remote-datastore-name", required=True, help="Datastore name")
    parser.add_argument(
        "--sqlite-file",
        help="Sqlite data file.  If given, use sqlite, otherwise memory."
        "  Must have the tables already created.",
    )
    parser.add_argument(
        "--print-results",
        action="store_true",
        help="If true, print documents to stdout",
    )
    parser.add_argument(
        "--tables", nargs="+", required=True, help="List of table names to sync"
    )
    parser.add_argument("--local-datastore-name", default="client", help="Datastore id")

    args = parser.parse_args()
    util.logging_basic_config(level=args.log_level)

    sqlite_conn = (
        sqlite3.connect(
            args.sqlite_file,
            # Use autocommit to not need transactions:
            isolation_level=None,
        )
        if args.sqlite_file
        else None
    )

    for table in args.tables:
        remote_datastore_name = args.remote_datastore_name + "/" + table
        remote_ds = RestClientSourceDatastore(args.server_url, remote_datastore_name)

        # Put docs in a local datastore
        ds = MemoryDatastore("client")
        if args.sqlite_file:
            ds = SqliteDatastore(args.local_datastore_name, sqlite_conn, table)
        with ds:
            Replicator(ds, remote_ds).sync_both_directions()

        if args.print_results:
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

        print(f"Local datastore has sequence id {ds.sequence_id} for table {table}")


if __name__ == "__main__":
    main()
