import sys

from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def _use_database(self, cursor, database_name):
        cursor.execute('USE {}'.format(database_name))

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        try:
            super()._execute_create_test_db(cursor, parameters, keepdb)
            # in case we're using the Express edition
            cursor.execute('ALTER DATABASE {} SET AUTO_CLOSE OFF'.format(parameters['dbname']))
        except Exception as e:
            if e.args[0] != '42000':
                # All errors except "database already exists" cancel tests.
                self.log('Got an error creating the test database: %s' % e)
                sys.exit(2)
            elif not keepdb:
                # If the database should be kept, ignore "database already
                # exists".
                raise

        self._use_database(cursor, parameters['dbname'])
