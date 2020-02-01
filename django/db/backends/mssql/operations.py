from django.db.backends.base.operations import BaseDatabaseOperations
from django.db import models


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        return '[{}]'.format(name)

    def max_name_length(self):
        return 128

    def last_insert_id(self, cursor, table_name, pk_name):
        # this should not be called directly, as the Id is returned directly from the insert statement
        raise NotImplementedError('Last inserted id should not be called directly')

    def bulk_insert_sql(self, fields, placeholder_rows, returning_fields):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)

        sql = ''

        if returning_fields:
            sql += 'OUTPUT ' + ', '.join(
                'INSERTED.{0}'.format(self.quote_name(f.column)) for f in returning_fields
            ) + ' '

        sql += 'VALUES ' + values_sql

        return sql


    def limit_offset_sql(self, low_mark, high_mark):
        return 'OFFSET {:d} ROWS FETCH FIRST {:d} ROWS ONLY'.format(
            low_mark, high_mark
        )

    def return_insert_columns(self, fields):
        return None, fields

    def fetch_returned_insert_rows(self, cursor):
        """
        Given a cursor object that has just performed an INSERT...OUTPUT...
        statement into a table, return the tuple of returned data.
        """
        return cursor.fetchall()

    def wrap_insert_sql(self, insert_sql, table_name, fields):
        # If we are inserting a value into identity column explicitly,
        # we need to turn on the identity insert and then immediately
        # turn if off
        columns_with_identity = (models.AutoField, models.BigAutoField, models.SmallAutoField)

        identity_insert = any(type(f) in columns_with_identity for f in fields)

        # TODO Handle errors in the insert, so the identity_on setting is not left hanging
        if identity_insert:
            statement, values = insert_sql[0]

            wrapped_statement = 'SET IDENTITY_INSERT {0} ON; {1}; SET IDENTITY_INSERT {0} OFF'.format(
                self.quote_name(table_name), statement
            )

            return [(wrapped_statement, values)]

        return insert_sql

    def start_transaction_sql(self):
        return 'BEGIN TRANSACTION'

    def end_transaction_sql(self, success=True):
        if not success:
            return 'ROLLBACK TRANSACTION'
        return 'COMMIT TRANSACTION'

    def savepoint_create_sql(self, sid):
        return 'SAVE TRANSACTION {0}'.format(
            self.quote_name(sid)
        )

    def savepoint_commit_sql(self, sid):
        return 'COMMIT TRANSACTION {0}'.format(
            self.quote_name(sid)
        )

    def savepoint_rollback_sql(self, sid):
        return 'ROLLBACK TRANSACTION {0}'.format(
            self.quote_name(sid)
        )

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"
