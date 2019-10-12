Introduction
------------

NOTE: This is unreleased software.  It's not done.

This is intended to be a simple implementation of synchronizing data
between two data sources with a "last write wins" policy.

I give no guarantee of correctness!  However, if it is correct, then
the result should be eventually consistent if peers synchronize with
each other regularly.

Currently, there are unchecked corner cases around concurrency.
For example, if one peer is synchronizing with two other peers at the
same time, that may not work, since there are multiple operations in
sync_both() and no transactional protection.

Tests
-----

To run tests:

```
$ docker-compose run data_sync nosetests sync_tests
```

You shouldn't have to give the "sync_tests", but
nosetests gets confused about the path.

To run tests and show logging from the `sync_tests` logger:

```
$ docker-compose run data_sync nosetests --debug=sync_tests sync_tests
```

See also [stackoverflow](https://stackoverflow.com/questions/32565562/make-nose-test-runner-show-logging-even-if-tests-pass).

To see the postgres DB:

```
$ docker-compose run db psql -h db -U postgres
```