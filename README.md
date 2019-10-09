Introduction
------------

This is intended to be a simple implementation of synchronizing data
between two data sources with a "last write wins" policy.

I give no guarantee of correctness!  However, if it is correct, then
the result should be eventually consistent if peers synchronize with
each other regularly.


Tests
-----

To run tests: `nosetests`
