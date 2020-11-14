from django.db.models.expressions import Subquery, Star
from django.db.models.sql import compiler
from django.db.models.sql.constants import SINGLE


class SQLCompiler(compiler.SQLCompiler):
    def collapse_group_by(self, expressions, having):
        expressions = super().collapse_group_by(expressions, having)

        # SQL Server does not allow constants to appear in GROUP BY clause

        # TODO Check if expressions with columns work
        return list(filter(lambda i: not isinstance(i, Subquery), expressions))

    def has_results(self):
        pk_column = self.query.model._meta.pk

        self.query.clear_limits()
        self.query.clear_ordering(True)
        self.query.add_select_col(pk_column.cached_col, pk_column.column)

        sql, params = self.as_sql()
        sql = self.connection.ops.result_exists_sql(sql)

        cursor = self.connection.cursor()

        cursor.execute(sql, params)

        results = cursor.fetchone()

        # TODO Check if works with joins
        return bool(results[0])

class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
