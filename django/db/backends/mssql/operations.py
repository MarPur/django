import uuid

from django.db import models
from django.db.models.expressions import Exists
from django.db.backends.base.operations import BaseDatabaseOperations


class DatabaseOperations(BaseDatabaseOperations):

    # Template to use to insert into a table
    # without providing any values and relying
    # on default values being generated
    insert_into_table_all_default_values = '''
        MERGE INTO {table}
        USING (SELECT *
        FROM (VALUES {row_placeholders}) t(_)) T
        ON 1 = 0
        WHEN NOT MATCHED THEN INSERT
          DEFAULT VALUES
    '''

    def quote_name(self, name):
        return '[{}]'.format(name)

    def max_name_length(self):
        return 128

    def bulk_batch_size(self, fields, objs):
        return 1000

    def last_insert_id(self, cursor, table_name, pk_name):
        # this should not be called directly, as the Id is returned directly from the insert statement
        raise NotImplementedError('Last inserted id should not be called directly')

    def bulk_insert_sql(self, fields, placeholder_rows, returning_fields):
        sql = ''

        if returning_fields:
            sql += 'OUTPUT ' + ', '.join(
                'INSERTED.{0}'.format(self.quote_name(f.column)) for f in returning_fields
            ) + ' '


        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)

        sql += 'VALUES ' + values_sql

        return sql

    def limit_offset_sql(self, low_mark, high_mark):
        fetch, offset = self._get_limit_offset_params(low_mark, high_mark)
        return 'OFFSET {:d} ROWS FETCH FIRST {:d} ROWS ONLY'.format(
            offset, fetch
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
        return 'SAVE TRANSACTION {0}'.format(sid)

    def savepoint_commit_sql(self, sid):
        # SQL Server does not support committing save points, i.e.,
        # parts of save transactions, instead it commits the entire transaction
        pass

    def savepoint_rollback_sql(self, sid):
        return 'ROLLBACK TRANSACTION {0}'.format(sid)

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'iregex', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    def conditional_expression_supported_in_where_clause(self, expression):
        if isinstance(expression, (Exists,)):
            return True
        return False

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
            return 'CAST(DATEADD(dd, -datepart(DAYOFYEAR, {0}) + 1, GETDATE()) AS DATE)'.format(field_name)
        elif lookup_type == 'day':
            return 'CAST({0} AS DATE)'.format(field_name)
        else:
            raise NotImplementedError('{0} is not implemented'.format(lookup_type))

    def insert_without_values(self, table_name, returning_fields, num_objects):
        row_placeholders = ', '.join('({0})'.format(i) for i in range(num_objects))

        sql = self.insert_into_table_all_default_values.format(
            table=self.quote_name(table_name), row_placeholders=row_placeholders
        )

        if returning_fields:
            sql += 'OUTPUT ' + ', '.join('INSERTED.{0}'.format(self.quote_name(f.name)) for f in returning_fields)

        sql += ';'

        return sql

    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        return converters

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value
