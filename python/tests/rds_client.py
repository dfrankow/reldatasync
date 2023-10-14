#!/usr/bin/env python3

import argparse
import logging

import requests
from reldatasync import util
from reldatasync.datastore import MemoryDatastore, RestClientSourceDatastore
from reldatasync.document import _ID, Document
from reldatasync.replicator import Replicator

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Test REST server.")
    parser.add_argument(
        "--server-url", "-s", dest="server_url", required=True, help="URL of the server"
    )
    parser.add_argument(
        "--log-level", "-l", dest="log_level", default="WARNING", help="Log level"
    )
    args = parser.parse_args()

    util.logging_basic_config(level=args.log_level)

    base_url = "http://" + args.server_url

    def server_url(url):
        return base_url + url

    # Create table1
    resp = requests.post(server_url("table1"))
    assert resp.status_code == 201

    # Check for table1
    resp = requests.get(server_url("table1"))
    assert resp.status_code == 200
    ct = resp.headers["content-type"]
    assert ct == "text/html; charset=utf-8", f"content type '{ct}'"
    assert resp.text == ""

    # Check for non-existent table2
    resp = requests.get(server_url("table2"))
    assert resp.status_code == 404

    # Check for docs in table1
    resp = requests.get(server_url("table1/docs"))
    assert resp.status_code == 200
    ct = resp.headers["content-type"]
    assert ct == "application/json", f"content type '{ct}'"
    js = resp.json()
    assert js["documents"] == []
    assert js["current_sequence_id"] == 0

    # Put three docs in table1
    d1 = Document({"_id": "1", "var1": "value1"})
    d2 = Document({"_id": "2", "var1": "value2"})
    d3 = Document({"_id": "3", "var1": "value3"})
    data = [d1, d2, d3]
    resp = requests.post(
        server_url("table1/docs"), params={"increment_rev": True}, json=data
    )
    assert resp.status_code == 200, resp.status_code
    ct = resp.headers["content-type"]
    assert ct == "application/json", f"content type '{ct}'"
    js = resp.json()
    assert js["num_docs_put"] == 3
    assert 3 == len(js["documents"])
    idx = 0
    for doc in js["documents"]:
        idx += 1
        assert str(idx) == doc["_id"], f"idx {idx} doc[_id] {doc['_id']}"
        assert doc["_rev"]

    # Put the same three docs in table1, num_docs_put==0
    # TODO: should we add increment_rev, and change server to check clocks?
    resp = requests.post(server_url("table1/docs"), json=data)
    assert resp.status_code == 422, resp.status_code
    assert (
        resp.content == b"doc 1 must have _rev if increment_rev is False"
    ), resp.content

    # Check three docs in table1
    resp = requests.get(server_url("table1/docs"))
    assert resp.status_code == 200
    ct = resp.headers["content-type"]
    assert ct == "application/json", f"content type '{ct}'"
    js = resp.json()
    assert len(js["documents"]) == 3, f"js is {js}"
    assert js["current_sequence_id"] == 3, f"js is {js}"
    docs = js["documents"]
    # server assigned revision numbers and sequence ids
    # compare data, except for _rev and _seq, set by server
    # also, they are returned in order
    for idx in range(len(data)):
        sdoc = docs[idx]
        doc = data[idx]
        doc["_rev"] = sdoc["_rev"]
        doc["_seq"] = sdoc["_seq"]
        assert doc in docs
        assert doc in data

    # Put docs in a local datastore
    ds = MemoryDatastore("client")
    # this id '1' will be different from table1 above, because we are
    # putting it in a different datastore with increment_rev=True
    d1a = Document({"_id": "1", "var1": "value1a"})
    d4 = Document({"_id": "4", "var1": "value4"})
    d5 = Document({"_id": "5", "var1": "value5"})
    for doc in [d1a, d4, d5]:
        num, new_doc = ds.put(Document(doc), increment_rev=True)
        assert num
        assert new_doc
        assert new_doc == ds.get(doc[_ID])

    # Sync local datastore with remote table1
    remote_ds = RestClientSourceDatastore(base_url, "table1")
    Replicator(ds, remote_ds).sync_both_directions()

    # Check that table1 and table2 have the same things
    assert ds.equals_no_seq(remote_ds)
    ds.check()
    remote_ds.check()


if __name__ == "__main__":
    main()
