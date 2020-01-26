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
