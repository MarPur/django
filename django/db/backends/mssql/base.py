import pyodbc as Database

from django.db.backends.base.base import BaseDatabaseWrapper

from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mssql'
    display_name = 'MS SQL Server'

    client_class = DatabaseClient
    features_class = DatabaseFeatures
    creation_class = DatabaseCreation
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    Database = Database

    def get_connection_params(self):
        kwargs = {
            'DRIVER': '{ODBC Driver 17 for SQL Server}'''
        }
        settings_dict = self.settings_dict

        if settings_dict['USER']:
            kwargs['Uid'] = settings_dict['USER']

        if settings_dict['NAME']:
            kwargs['Database'] = settings_dict['NAME']

        if settings_dict['PASSWORD']:
            kwargs['Pwd'] = settings_dict['PASSWORD']

        if settings_dict['HOST']:
            kwargs['Server'] = settings_dict['HOST']

        return kwargs

    def get_new_connection(self, conn_params):
        connection_string = ';'.join('{}={}'.format(k, v) for (k, v) in conn_params.items())

        return Database.connect(connection_string)

    def init_connection_state(self):
        pass

    def create_cursor(self, name=None):
        return self.connection.cursor()

    def _set_autocommit(self, autocommit):
        self.connection.autocommit = autocommit
