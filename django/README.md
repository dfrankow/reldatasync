Django utilities for syncable data.

Development
-----------

To install the reldatasync python package:

```
$ python -m pip install ../python/
```

To test installing the django-reldatasync python package:

```
$ python -m pip install -e .
```

To configure for testing:

```
export SECRET_KEY=a-secret-key

# a database URL, such as this postgres example:
export DATABASE_URL=postgres://user@localhost/reldatasyncdb
```
