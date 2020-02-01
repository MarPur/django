from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    requires_order_by_in_limit = True
    supports_transactions = True
    can_return_rows_from_bulk_insert = True
    can_return_columns_from_insert = True
