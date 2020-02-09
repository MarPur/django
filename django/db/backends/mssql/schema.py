from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    def quote_value(self, value):
        if isinstance(value, bool):
            return "1" if value else "0"
        else:
            return str(value)
