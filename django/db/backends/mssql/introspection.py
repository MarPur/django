from django.db.models import Index
from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, TableInfo,
)


class DatabaseIntrospection(BaseDatabaseIntrospection):

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
                    'foreign_key': False,
                    'check': True,
                    'index': False
                }

            if column:
                constraints[constraint]['columns'].append(column)

        # Foreign Keys
        # cursor.execute('''
        #     SELECT
        #         OBJECT_NAME(fkc.constraint_object_id) constraint_name,
        #         OBJECT_NAME(fkc.referenced_object_id) AS [referenced_table],
        #         c2.name AS referenced_column
        #     FROM sys.foreign_key_columns fkc
        #     INNER JOIN sys.columns c ON c.column_id = parent_column_id AND c.object_id = fkc.parent_object_id
        #     INNER JOIN sys.columns c2 ON c2.column_id = referenced_column_id AND c2.object_id = fkc.referenced_object_id
        #     WHERE OBJECT_NAME(fkc.parent_object_id) = '{0}'
        # '''.format(table_name))
        #
        # for constraint, referenced_table, referenced_column in cursor.fetchall():
        #     if constraint not in constraints:
        #         constraints[constraint] = {
        #             'columns': [],
        #             'primary_key': False,
        #             'unique': False,
        #             'foreign_key': (referenced_table, referenced_column),
        #             'check': False,
        #             'index': False,
        #         }
        #
        #     constraints[constraint]['columns'].append(column)

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
                    'foreign_key': False,
                    'check': False,
                    'index': is_index,
                    'type': Index.suffix if is_index else None
                }

            constraints[constraint]['columns'].append(column)
            constraints[constraint]['orders'].append('DESC' if is_descending else 'ASC')

        return constraints
