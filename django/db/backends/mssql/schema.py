from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_delete_table = 'DROP TABLE %(table)s'
    sql_delete_column = 'ALTER TABLE %(table)s DROP COLUMN %(column)s'
    sql_rename_table = "EXEC sp_rename '%(old_table)s', '%(new_table)s'"

    def _alter_column_default_sql(self, model, old_field, new_field, drop=False):
        if drop:
            # In SQL Server, DEFAULT is a constraint, so we need to find the DEFAULT constraint
            # for the given table and column and drop the constraint.
            with self.connection.cursor() as cursor:
                result = cursor.execute('''
                    SELECT OBJECT_NAME(c.object_id) AS [constraint]
                    FROM sys.default_constraints c
                    INNER JOIN sys.columns col ON col.object_id = c.parent_object_id AND col.column_id = c.parent_column_id
                    WHERE col.name = '{0}' AND OBJECT_NAME(c.parent_object_id) = '{1}'
                '''.format(new_field.column, model._meta.db_table)).fetchone()

                if result:
                    return 'DROP CONSTRAINT [{0}]'.format(result[0]), ()
                return None, ()

        return super(model, old_field, new_field, drop=False)

    def prepare_default(self, value):
        if isinstance(value, bool):
            return int(value)
        return self.quote_value(value)

    def quote_value(self, value):
        if isinstance(value, str):
            return "'{0}'".format(value.replace("'", "\'"))
        return str(value)

    def alter_db_table(self, model, old_db_table, new_db_table):
        if (old_db_table == new_db_table or
            (self.connection.features.ignores_table_name_case and
                old_db_table.lower() == new_db_table.lower())):
            return
        self.execute(self.sql_rename_table % {
            "old_table": old_db_table,
            "new_table": new_db_table,
        })

        for sql in self.deferred_sql:
            if isinstance(sql, Statement):
                sql.rename_table_references(old_db_table, new_db_table)
