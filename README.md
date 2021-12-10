Introduction
------------

NOTE: This is unreleased software.  It's not done.

This is intended to be a simple implementation of synchronizing data
between two relational data sources (i.e., tables with columns),
with a "last write wins" policy.

I give no guarantee of correctness!  However, if it is correct, then
the result should be eventually consistent if peers synchronize with
each other regularly.

Currently, there are unchecked corner cases around concurrency.
For example, if one peer is synchronizing with two other peers at the
same time, that may not work, since there are multiple operations in
sync_both() and no transactional protection.


Tests without docker
--------------------

Set POSTGRES_HOST and POSTGRES_USER.

The tests will create and delete databases (e.g., named "test_server"
and "test_client"), so only give permissions to a Postgres instance
that is safe for that operation.

```
export POSTGRES_HOST=...
export POSTGRES_USER=...
```

Run sync_tests:

```
python -m unittest discover -s tests 
```


Tests using docker
------------------

To run tests in the development environment:

```
$ docker-compose run --rm data_sync python setup.py test
```

or directly:

```
$ docker-compose up
$ docker-compose run data_sync nosetests tests/test_sync.py
```

You shouldn't have to give the "test_sync.py", but
nosetests gets confused about the path.

To run tests and show logging from the `test_sync` logger (BROKEN):

```
$ docker-compose run data_sync nosetests --debug=tests.test_sync tests/test_sync.py
```

See also [stackoverflow](https://stackoverflow.com/questions/32565562/make-nose-test-runner-show-logging-even-if-tests-pass).

To see the postgres DB:

```
$ docker-compose run db psql -h db -U postgres
```

Test client and server
----------------------

To run a test that starts the server and applies client.py:

```
$ docker-compose run --rm data_sync /app/tests/test_server_and_client.py 
```


To run the test server by itself (accessible on 0.0.0.0):

```
$ docker-compose run --service-ports data_sync env \
    FLASK_APP=tests/server.py flask run -h 0.0.0.0
```

It listens to host `0.0.0.0` to get localhost messages from outside
the Docker container.

To run the test client (that connects to server.py):

```
$ docker-compose run data_sync tests/client.py -s 172.22.0.3:5000
```

It will work only once, because it requires an empty DB on the server.

I get the IP address by looking in the docker network:

```
$ docker network inspect data_sync_default
```

There must be a better way than grabbing the IP of the server.


Related efforts
---------------

There are many projects and concepts that relate to this type of data
syncing.

[SymmetricDS](https://symmetricds.org) is an open source project to synchronize
data.  However, a recent download of the server package is over 100M.
I hope this can be smaller and more nimble for my focused use case.

[CouchDB](https://couchdb.apache.org/) is another open source project to
synchronize data.  However, it requires the Erlang runtime, and implements
its own NoSQL document store.  I would like to have something I can use
with my common stack: Postgres on a server, and sqlite on android.

CouchDB has a nice property that every revision is always kept and
synced.  However, this makes querying more complicated, as you always
have to ignore old revisions.  Thus, I've decided to only keep the
latest revision ("last write wins"), which makes this effort not
compatible with CouchDB.

[Cloud Firestore](https://firebase.google.com/products/firestore/) is a
product that handles synchronization including an offline mode.  However,
it is a NoSQL data store.

[AMPLI-SYNC](https://github.com/sqlite-sync/SQLite-sync.com) looks very
close to what I want, but it requires Tomcat (a Java server), and I want
to use Django.
