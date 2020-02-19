from django.db.models import CharField, FileField, FilePathField, SlugField, UUIDField, TextField

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_delete_table = 'DROP TABLE %(table)s'
    sql_delete_column = 'ALTER TABLE %(table)s DROP COLUMN %(column)s'
    sql_rename_table = "EXEC sp_rename '%(old_table)s', '%(new_table)s'"

    def _column_default_sql(self, field):
        if type(field) in (CharField, FileField, FilePathField, SlugField, UUIDField, TextField):
            return "'%s'"
        return '%s'

    def execute(self, sql, params=()):
        if params:
            return super().execute(sql % tuple(params))
        return super().execute(sql)

    def quote_value(self, value):
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
