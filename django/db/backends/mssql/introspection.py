from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, TableInfo,
)


class DatabaseIntrospection(BaseDatabaseIntrospection):

    def get_table_list(self, cursor):
        cursor.execute("""
            SELECT TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 't' ELSE 'v' END AS OBJECT_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        """)

        return [TableInfo(row[0], row[1]) for row in cursor.fetchall()]

    def get_constraints(self, cursor, table_name):
        constraints = {}

        # Unique and Primary Key
        cursor.execute('''
            SELECT
                ku.column_name,
                ku.constraint_name,
                tc.constraint_type
            FROM information_schema.table_constraints AS tc
            INNER JOIN information_schema.key_column_usage AS ku ON
                tc.constraint_type IN('PRIMARY KEY', 'UNIQUE') AND tc.constraint_name = ku.constraint_name
            WHERE tc.table_name = '{0}'
            ORDER BY ku.table_name, KU.ordinal_position;
        '''.format(table_name))

        for column, constraint, constraint_type in cursor.fetchall():
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': [],
                    'primary_key': constraint_type == 'PRIMARY KEY',
                    'unique': constraint_type == 'UNIQUE',
                    'foreign_key': False,
                    'check': constraint_type == 'CHECK',
                    'index': False
                }

            constraints[constraint]['columns'].append(column)

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

        # TODO Indexes

        return constraints
