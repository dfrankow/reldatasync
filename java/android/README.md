This directory contains Java source for a client to sync using HTTP.

Note: it uses OkHttp, which explicitly has requirements for old
Android versions.  https://square.github.io/okhttp/#requirements says:

> OkHttp works on Android 5.0+ (API level 21+) and Java 8+.


Building
--------

- Download Android Studio
- Open android project

OR

- run ./gradlew <task>

`task` can be `tasks` to see the list of possible tasks to run.


Testing
-------

There are some unit tests.

Also, there is a TestClient that exercises some functionality.

To run TestClient, first start a server:

```
source .venv/bin/activate
export PYTHONPATH=../reldatasync
./tests/rds_server.py
```

Then run TestClient in the IDE.
