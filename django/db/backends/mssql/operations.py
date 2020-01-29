from django.db.backends.base.operations import BaseDatabaseOperations


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        return '[{}]'.format(name)

    def max_name_length(self):
        return 128
