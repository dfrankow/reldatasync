Tests without docker
--------------------

Set POSTGRES_HOST and POSTGRES_USER.

The tests will create and delete databases (e.g., named "test_server"
and "test_client"), so only give permissions to a Postgres instance
that is safe for that operation.

```bash
export POSTGRES_HOST=...
export POSTGRES_USER=...
```

Put `reldatasync/python` into the path:

```bash
export PYTHONPATH=`pwd`
```

Run tests:

```bash
python -m unittest discover -s tests
```

Run tests in one file with:

```bash
python -m unittest tests.test_document
```


REST protocol
=============

This is the REST protocol expected of an HTTP server.


- `/<datastore>/doc/<docid>?include_deleted=<true|false>`
GET a doc from datastore with given docid.
`include_deleted`: if true, include deleted docs.  Default: false.

- `/<datastore>/doc?increment_rev=<true|false>`
POST a doc to the datastore.
`increment_rev`: if true, add a revision to the doc, otherwise fail if one is
   not present.  Default: false.
Return `{"document": doc, "num_docs_put": <int>}`.

- `/<datastore>/docs?start_sequence_id=<int>&chunk_size=<int>`
GET docs put with `start_sequence_id < _seq <= (start_sequence_id+chunk_size)`
Return `{"current_sequence_id": cur_seq_id, "documents": the_docs}`

POST a json array of docs.
