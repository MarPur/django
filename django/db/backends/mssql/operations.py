import datetime
import uuid

import pytz

from django.conf import settings
from django.db import models
from django.db.models.expressions import Exists, RawSQL, ExpressionWrapper
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django.db.backends.mssql.compiler"

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

    records_exists_wrapping_query = '''
        SELECT CASE WHEN EXISTS ({query}) THEN 1 ELSE 0 END AS _exists
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

    def result_exists_sql(self, query):
        return self.records_exists_wrapping_query.format(query=query)

    def limit_offset_sql(self, low_mark, high_mark):
        fetch, offset = self._get_limit_offset_params(low_mark, high_mark)

        return ' '.join(sql for sql in (
            ('OFFSET {0} ROWS'.format(offset or 0)),
            ('FETCH FIRST {0} ROWS ONLY'.format(fetch)) if fetch else None,
        ) if sql)

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
        if isinstance(expression, (Exists, RawSQL, ExpressionWrapper)):
            return True
        return False

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
            return 'CAST(DATEADD(DAY, -DATEPART(DAYOFYEAR, {0}) + 1, {0}) AS DATE)'.format(field_name)
        elif lookup_type == 'month':
            return 'CAST(DATEADD(DAY, -DATEPART(DAY, {0}) + 1, {0}) AS DATE)'.format(field_name)
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
        if internal_type == 'DateTimeField':
            if settings.USE_TZ:
                converters.append(self.convert_datetimefield_value)

        return converters

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_datetimefield_value(self, value, expression, connection):
        if value is None:
            return

        return timezone.make_aware(value, self.connection.timezone)

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None

        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = timezone.make_naive(value, self.connection.timezone)
            else:
                raise ValueError("SQL Server backend does not support timezone-aware datetimes when USE_TZ is False.")

        return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def last_executed_query(self, cursor, sql, params):
        if not params:
            return sql
        return sql % params

    def _get_referencing_constrains(self, table_name, recursive):
        with self.connection.cursor() as cursor:
            return cursor.execute('''
                with ForeignKeys AS (
                    SELECT
                           OBJECT_NAME(fk.object_id) constrain_name
                         , fk.object_id AS constraint_object_id
                         , OBJECT_NAME(fk.parent_object_id) referencing_table
                         , fk.parent_object_id AS referencing_table_object_id
                         , OBJECT_NAME(fk.referenced_object_id) referenced_table
                         , fk.referenced_object_id AS referenced_table_object_id
                         , 1 AS Level
                    FROM sys.foreign_keys fk
                    WHERE OBJECT_NAME(fk.referenced_object_id) = '{table}'

                    UNION ALL

                    SELECT
                           OBJECT_NAME(child.object_id)
                         , child.object_id
                         , OBJECT_NAME(child.parent_object_id)
                         , child.parent_object_id
                         , OBJECT_NAME(child.referenced_object_id)
                         , child.referenced_object_id
                         , parent.Level + 1
                    FROM sys.foreign_keys AS child
                    INNER JOIN ForeignKeys parent ON child.referenced_object_id = parent.referencing_table_object_id AND parent.referenced_table_object_id != child.parent_object_id
                )
                SELECT DISTINCT constrain_name, referencing_table, referenced_table, level
                FROM ForeignKeys
                WHERE Level = 1 AND 1 = {not_recursive} OR 1 = {recursive}
                ORDER BY LEVEL DESC;
            '''.format(table=table_name, recursive=int(recursive), not_recursive=int(not recursive))).fetchall()

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        statements = []

        if not tables:
            return statements

        # if the allow_cascade is true, recursively, truncate tables
        # that reference the tables in `tables`. Otherwise, drop the
        # foreign key constraints

        for table in sorted(tables):
            referencing_objects = self._get_referencing_constrains(table, allow_cascade)

            if allow_cascade:
                statements.extend(
                    '{0} {1} {2}'.format(
                        style.SQL_KEYWORD('DELETE'),
                        style.SQL_KEYWORD('FROM'),
                        style.SQL_FIELD(self.quote_name(row[1])),
                    ) for row in referencing_objects
                )
            else:
                statements.extend(
                    '{0} {1} {2} {3} {4} {5}'.format(
                        style.SQL_KEYWORD('ALTER'),
                        style.SQL_KEYWORD('TABLE'),
                        style.SQL_FIELD(self.quote_name(row[1])),
                        style.SQL_KEYWORD('DROP'),
                        style.SQL_KEYWORD('CONSTRAINT'),
                        style.SQL_FIELD(self.quote_name(row[0])),
                    ) for row in referencing_objects
                )

            statements.append(
                '{0} {1} {2}'.format(
                    style.SQL_KEYWORD('DELETE'),
                    style.SQL_KEYWORD('FROM'),
                    style.SQL_FIELD(self.quote_name(table)),
                )
            )

        return statements

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)

        return self.date_extract_sql(lookup_type, field_name)

    def date_extract_sql(self, lookup_type, field_name):
        # TODO Check extracting date & time
        if lookup_type == 'week_day':
            return 'DATEPART(WEEKDAY, {0})'.format(field_name)
        elif lookup_type == 'iso_week_day':
            return '((DATEPART(WEEKDAY, {0}) + 5) %% 7 + 1)'.format(field_name)
        elif lookup_type == 'iso_year':
            return 'DATEPART(YEAR, {0})'.format(field_name)
        elif lookup_type == 'iso_year':
            return 'DATEPART(YEAR, {0})'.format(field_name)
        elif lookup_type == 'week':
            return 'DATEPART(ISO_WEEK, {0})'.format(field_name)
        else:
            return 'DATEPART({0}, {1})'.format(lookup_type, field_name)

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)

        if lookup_type == 'year':
            return 'CAST(CAST(DATEADD(dd, -DATEPART(DAYOFYEAR, {0}) + 1, {0}) AS DATE) AS DATETIME)'.format(field_name)

        if lookup_type == 'month':
            return 'CAST(CAST(DATEADD(dd, -DATEPART(DAY, {0}) + 1, {0}) AS DATE) AS DATETIME)'.format(field_name)

        if lookup_type == 'day':
            return 'CAST(CAST({0} AS DATE) AS DATETIME)'.format(field_name)

        if lookup_type == 'hour':
            return 'DATEADD(MI, -DATEPART(MI, {0}), DATEADD(S, -DATEPART(S, {0}), DATEADD(MS, -DATEPART(MS, {0}), {0})))'.format(field_name)

        if lookup_type == 'minute':
            return 'DATEADD(S, -DATEPART(S, {0}), DATEADD(MS, -DATEPART(MS, {0}), {0}))'.format(field_name)

        if lookup_type == 'second':
            return 'DATEADD(MS, -DATEPART(MS, {0}), {0})'.format(field_name)

        raise NotImplementedError('{0} is not implemented'.format(lookup_type))

    def datetime_cast_date_sql(self, field_name, tzname):
        field_name = self._convert_field_to_tz(field_name, tzname)
        return 'CAST({0} AS DATE)'.format(field_name)

    def _convert_field_to_tz(self, field_name, tzname):
        # SQL Server does not have tz database, instead
        # it reads available timezones from the machine's registry
        # https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-time-zone-info-transact-sql?view=sql-server-ver15
        # so we try to calculate the offsets in python
        # rather than in the DB
        if settings.USE_TZ and  self.connection.timezone_name != tzname:
            connection_tz_offset = self._get_timezone_offset(self.connection.timezone_name)
            current_tz_offset = self._get_timezone_offset(tzname)

            offset = current_tz_offset - connection_tz_offset

            field_name = 'DATEADD(second, {0}, {1})'.format(offset, field_name)
        return field_name

    def _get_timezone_offset(self, tzname):
        zone = pytz.timezone(tzname)
        now = datetime.datetime.now()
        delta = zone.localize(now, is_dst=False).utcoffset()
        return delta.days * 86400 + delta.seconds

    def date_interval_sql(self, timedelta):
        # SQL Server interprets the number when adding to DATETIME as the number of days
        return str(timedelta.days + (timedelta.seconds + timedelta.microseconds / 1000000) / (24 * 60 * 60))

    def no_limit_value(self):
        return None

    def format_for_duration_arithmetic(self, sql):
        # Convert microseconds to days
        return '{0} / (24.0 * 60 * 60 * 1000000)'.format(sql)
