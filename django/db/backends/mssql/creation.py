import sys

from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def _init_database(self, cursor, database_name):
        cursor.execute('ALTER DATABASE {0} SET AUTO_CLOSE OFF'.format(database_name))
        cursor.execute('ALTER DATABASE {0} SET MULTI_USER'.format(database_name))

        cursor.execute('USE {0}'.format(database_name))

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        database_name = parameters['dbname']

        try:
            super()._execute_create_test_db(cursor, parameters, keepdb)
            # in case we're using the Express edition
        except Exception as e:
            if e.args[0] != '42000':
                # All errors except "database already exists" cancel tests.
                self.log('Got an error creating the test database: %s' % e)
                sys.exit(2)
            elif not keepdb:
                # If the database should be kept, ignore "database already
                # exists".
                raise

        self._init_database(cursor, database_name)

        return database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        # kick every other session out of the database, so we could safely drop it
        with self.connection._nodb_connection.cursor() as cursor:
            cursor.execute('ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE'.format(test_database_name))
            cursor.execute('DROP DATABASE IF EXISTS [{0}]'.format(test_database_name))
