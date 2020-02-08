from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    supports_timezones = False
    requires_order_by_in_limit = True
    supports_transactions = True
    can_return_rows_from_bulk_insert = True
    can_return_columns_from_insert = True
    supports_nullable_unique_constraints = False
    supports_ignore_conflicts = False
    supports_bulk_inserts_without_values = False
    supports_paramstyle_pyformat = False
    supports_subqueries_in_group_by = False
