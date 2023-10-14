Introduction
------------

NOTE: This is unreleased software.  It's not done.

This is intended to be a simple implementation of synchronizing data
between two relational data sources (i.e., tables with columns),
with a "last write wins" policy, specifically to support offline-first
(i.e. often disconnected) applications.

I give no guarantee of correctness!  However, if it is correct, then
the result should be eventually consistent if peers synchronize with
each other regularly.

Currently, there are unchecked corner cases around concurrency.
For example, if one peer is synchronizing with two other peers at the
same time, that may not work, since there are multiple operations in
sync_both() and no transactional protection.


Code structure
--------------

Subdirectories:

- python - A python package (reldatasync)
- django - A django package (django-reldatasync)
- java/android - Code suitable for android


Dependencies
------------

I don't know a whole lot about how to make good dependencies for a
library.  However, I am going to try using the pip ~= operator, which
allows some small flexibility.  See also
[here](https://stackoverflow.com/questions/39590187/in-requirements-txt-what-does-tilde-equals-mean).


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
have to ignore old revisions.

Thus, I've decided to only keep the latest revision ("last write
wins"), which makes this effort not compatible with CouchDB.

For more on using vector clocks to implement "last write wins", see
for example section 4.4 ("Data Versioning") of [Dynamo: Amazon’s
Highly Available Key-value
Store](https://www.allthingsdistributed.com/2007/10/amazons_dynamo.html).

[Cloud Firestore](https://firebase.google.com/products/firestore/) is
a product that handles synchronization including [an offline
mode](https://firebase.google.com/docs/database/android/offline-capabilities#section-offline-behavior).
(See [this stackoverflow post](https://stackoverflow.com/a/52912231)
for more information about conflict resolution.)  However, it is a
NoSQL data store.

[AMPLI-SYNC](https://github.com/sqlite-sync/SQLite-sync.com) looks very
close to what I want, but it requires Tomcat (a Java server), and I want
to use Django.
