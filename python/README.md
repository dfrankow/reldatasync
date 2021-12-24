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

Run tests with either of the following two commands:

```bash
python -m unittest discover -s tests
python setup.py test
```

Run tests in one file with:

```bash
python -m unittest tests.test_vectorclock
```

Tests using docker (deprecated)
-------------------------------

I haven't done this in awhile.

To run tests in the development environment:

```
$ docker-compose run --rm data_sync python setup.py test
```

or directly:

```bash
$ docker-compose up
$ docker-compose run data_sync nosetests tests/test_datastore.py
```

You shouldn't have to give the "test_sync.py", but
nosetests gets confused about the path.

To run `test_sync.py` with DEBUG logging:

```bash
$ docker-compose run data_sync bash -c "LOG_LEVEL=DEBUG python -m unittest tests.test_sync"
```

To see the postgres DB:

```bash
$ docker-compose run db psql -h db -U postgres
```

Test client and server with docker (deprecated)
-----------------------------------------------

I don't do this much.  The unit test runner without docker will also
run this test.


To run a test that starts the server and applies client.py:

```bash
$ docker-compose run --rm data_sync /app/tests/test_server_and_client.py
```


To run the test server by itself (accessible on 0.0.0.0):

```bash
$ docker-compose run --service-ports data_sync env \
    FLASK_APP=tests/server.py flask run -h 0.0.0.0
```

It listens to host `0.0.0.0` to get localhost messages from outside
the Docker container.

To run the test client (that connects to server.py):

```bash
$ docker-compose run data_sync tests/client.py -s 172.22.0.3:5000
```

It will work only once, because it requires an empty DB on the server.

I get the IP address by looking in the docker network:

```bash
$ docker network inspect data_sync_default
```

There must be a better way than grabbing the IP of the server.
