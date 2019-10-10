Introduction
------------

This is intended to be a simple implementation of synchronizing data
between two data sources with a "last write wins" policy.

I give no guarantee of correctness!  However, if it is correct, then
the result should be eventually consistent if peers synchronize with
each other regularly.


Tests
-----

To run tests:

```
$ docker-compose run data_sync nosetests sync_tests
```

You shouldn't have to give the "sync_tests", but
nosetests gets confused about the path.
