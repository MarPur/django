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

        # TODO The returned ID could belong to another session. Check if we can use SCOPE_IDENTITY() instead
        result = cursor.execute("SELECT IDENT_CURRENT(%s)", (table_name,)).fetchone()

        return result[0]

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def limit_offset_sql(self, low_mark, high_mark):
        return 'OFFSET {:d} ROWS FETCH FIRST {:d} ROWS ONLY'.format(
            low_mark, high_mark
        )
