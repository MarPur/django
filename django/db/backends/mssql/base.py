import pyodbc as Database

from django.db.backends.base.base import BaseDatabaseWrapper

from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mssql'
    display_name = 'MS SQL Server'

    client_class = DatabaseClient
    features_class = DatabaseFeatures
    creation_class = DatabaseCreation
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor

    data_types = {
        'AutoField': 'int, identity(1,1)',
        'BigAutoField': 'bigint, identity(1,1)',
        'BinaryField': 'varbinary(MAX)',
        'BooleanField': 'bit',
        'CharField': 'nvarchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'datetimeoffset', # Use simple datetime?
        'DecimalField': 'decimal(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'bigint',
        'FileField': 'nvarchar(%(max_length)s)',
        'FilePathField': 'nvarchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'real',
        'BigIntegerField': 'bigint',
        'GenericIPAddressField': 'varchar(39)',
        'NullBooleanField': 'bit',
        'OneToOneField': 'int',
        'PositiveBigIntegerField': 'bigint',
        'PositiveIntegerField': 'int',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'nvarchar(%(max_length)s)',
        'SmallAutoField': 'smallint identity(1,1)',
        'SmallIntegerField': 'smallint',
        'TextField': 'nvarchar(max)',
        'TimeField': 'time',
        'UUIDField': 'char(32)',
    }

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
