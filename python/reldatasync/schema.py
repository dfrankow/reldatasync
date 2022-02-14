class Schema:
    def __init__(self, field_types):
        """A schema is a dict of field name to type name.

        NOTE: the schema is strongly based on sqlite3,
        but adds boolean and datetime
        See also https://www.sqlite.org/datatype3.html.

        Current types: INTEGER, REAL, TEXT, BOOLEAN, DATE, DATETIME.
        """
        self._field_types = field_types

    def field_type(self, field):
        return self._field_types[field]

    def set_field_type(self, field, the_type):
        self._field_types[field] = the_type
