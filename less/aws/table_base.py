import random

CHARS_FOR_KEY = "ACDEFGHIJKLMNPQRSTUVWXYZabcdefghijkmnpqrsuvwxyz2345679"


def generate_id():
    return "".join(random.choices(CHARS_FOR_KEY, k=10))


class InputError(Exception):
    def __init__(self, message):
        self.message = message


class TableBase(object):
    MAX_BATCH = 1000

    def _validate_primary_key(self, key):
        if not key:
            raise InputError("Missing key")
        for k in self.table_configuration.primary_key:
            if k not in key:
                raise InputError(f"Missing '{k}'")

    @property
    def is_sql_query_supported(self):
        return False

    def sql_query(self, sql, params):
        raise NotImplementedError()

    @property
    def is_paging_supported(self):
        return False
