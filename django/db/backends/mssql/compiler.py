from django.db.models.sql import compiler
from django.db.models.expressions import Ref

from django.utils.hashable import make_hashable


class SQLCompiler(compiler.SQLCompiler):
    def collapse_group_by(self, expressions, having):
        expressions = super().collapse_group_by(expressions, having)

        # SQL Server does not allow constants to appear in GROUP BY clause

        # TODO Check if expressions with columns work
        return list(filter(lambda i: hasattr(i.output_field, 'model'), expressions))


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
