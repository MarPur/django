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
