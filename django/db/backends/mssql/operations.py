from django.db.backends.base.operations import BaseDatabaseOperations


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        return '[{}]'.format(name)

    def max_name_length(self):
        return 128

    def last_insert_id(self, cursor, table_name, pk_name):
        """
        Given a cursor object that has just performed an INSERT statement into
        a table that has an auto-incrementing ID, return the newly created ID.
        """

        # TODO N.B. The returned ID could belong to another session. Check if we can use SCOPE_IDENTITY() instead
        result = cursor.execute("SELECT IDENT_CURRENT(%s)", (table_name,)).fetchone()

        return result[0]
