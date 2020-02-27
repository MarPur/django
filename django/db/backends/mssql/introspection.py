from collections import namedtuple

from django.db.models import Index
from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo as BaseFieldInfo, TableInfo,
)

FieldInfo = namedtuple('FieldInfo', BaseFieldInfo._fields + ('is_identity',))


class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = {
        'varbinary': 'BinaryField',
        'bit': 'BooleanField',
        'nvarchar': 'CharField',
        'date': 'DateField',
        'datetime2': 'DateTimeField',
        'decimal': 'DecimalField',
        'real': 'FloatField',
        'int': 'IntegerField',
        'bigint': 'BigIntegerField',
        'smallint': 'SmallIntegerField',
        'time': 'TimeField',
    }

    identity_data_types_reverse = {
        'int': 'AutoField',
        'bigint': 'BigAutoField',
        'smallint': 'SmallAutoField',
    }

    def get_field_type(self, data_type, description):
        if description.is_identity:
            return self.identity_data_types_reverse[data_type]

        return self.data_types_reverse[data_type]

    def get_table_list(self, cursor):
        cursor.execute("""
            SELECT TABLE_NAME, CASE WHEN table_type = 'BASE TABLE' THEN 't' ELSE 'v' END AS object_type
            FROM information_schema.tables
            WHERE table_type IN ('BASE TABLE', 'VIEW')
        """)

        return [TableInfo(row[0], row[1]) for row in cursor.fetchall()]

    def get_constraints(self, cursor, table_name):
        constraints = {}

        # Check constraints
        cursor.execute('''
            SELECT
                OBJECT_NAME(c.object_id),
                col.[name] as column_name
            FROM sys.check_constraints c
            LEFT JOIN sys.columns col ON c.parent_column_id = col.column_id AND c.parent_object_id = col.object_id
            WHERE OBJECT_NAME(parent_object_id) = '{0}'
            ORDER BY c.name
        '''.format(table_name))

        for constraint, column in cursor.fetchall():
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': True,
                    'index': False
                }

            if column:
                constraints[constraint]['columns'].append(column)

        # Indexes
        cursor.execute('''
            SELECT i.name, i.is_primary_key, i.is_unique, c.name AS [column], ic.is_descending_key
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id
            INNER JOIN sys.columns c ON c.column_id = ic.column_id AND ic.object_id = c.object_id
            WHERE OBJECT_NAME(i.object_id) = '{0}'
            ORDER BY i.name, ic.key_ordinal
        '''.format(table_name))

        for constraint, is_pk, is_unique, column, is_descending in cursor.fetchall():
            if constraint not in constraints:
                is_index = not is_pk and not is_unique

                constraints[constraint] = {
                    'columns': [],
                    'orders': [],
                    'primary_key': is_pk,
                    'unique': is_unique,
                    'foreign_key': None,
                    'check': False,
                    'index': is_index,
                    'type': Index.suffix if is_index else None
                }

            constraints[constraint]['columns'].append(column)
            constraints[constraint]['orders'].append('DESC' if is_descending else 'ASC')

        return constraints

    def get_key_columns(self, cursor, table_name):
        cursor.execute('''
            SELECT
                c.name AS referencing_column,
                OBJECT_NAME(fkc.referenced_object_id) AS [referenced_table],
                c2.name AS referenced_column
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.columns c ON c.column_id = parent_column_id AND c.object_id = fkc.parent_object_id
            INNER JOIN sys.columns c2 ON c2.column_id = referenced_column_id AND c2.object_id = fkc.referenced_object_id
            WHERE OBJECT_NAME(fkc.parent_object_id) = '{0}'
        '''.format(table_name))

        return list(cursor.fetchall())

    def get_relations(self, cursor, table_name):
        constraints = self.get_key_columns(cursor, table_name)
        relations = {}
        for my_fieldname, other_table, other_field in constraints:
            relations[my_fieldname] = (other_field, other_table)
        return relations

    def get_table_description(self, cursor, table_name):
        results = cursor.execute('''
            SELECT col.name AS [column], t.name AS type_name, COL_LENGTH(OBJECT_NAME(col.object_id), col.name) AS size,
                col.precision, col.scale, col.is_nullable, c.definition AS constraint_expression, col.is_identity
            FROM sys.columns col
            INNER JOIN sys.types t ON t.system_type_id = col.system_type_id
            LEFT JOIN sys.default_constraints c ON col.object_id = c.parent_object_id AND col.column_id = c.parent_column_id
            WHERE OBJECT_NAME(col.object_id) = '{0}' AND t.name NOT IN ('sysname')
            ORDER BY col.column_id
        '''.format(table_name)).fetchall()

        fields = []
        for result in results:
            fields.append(FieldInfo(
                result[0],
                result[1],
                None,
                result[2],
                result[3],
                result[4],
                result[5],
                result[6],
                result[7]
            ))

        return fields

    def get_sequences(self, cursor, table_name, table_fields=()):
        description = self.get_table_description(cursor, table_name)

        return [{'table': table_name, 'column': i.name} for i in description if i.is_identity]

