class InputError(Exception):
    def __init__(self, message):
        self.message = message


class TableBase(object):
    def _validate_primary_key(self, key):
        if not key:
            raise InputError("Missing key")
        for k in self.table_configuration.primary_key:
            if k not in key:
                raise InputError(f"Missing '{k}'")
