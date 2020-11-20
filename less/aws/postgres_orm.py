import psycopg2
from psycopg2.extras import RealDictCursor

from less.aws.table_base import InputError, generate_id, TableBase


class PostgresTable(TableBase):
    def __init__(self, table_configuration, connection_info):
        self.table_configuration = table_configuration
        self.attributes_by_name = {
            k["name"]: k for k in self.table_configuration.attributes
        }
        self.connection_info = connection_info

    def connect(self):
        return psycopg2.connect(
            dbname=self.connection_info["db"],
            host=self.connection_info["host"],
            user=self.connection_info["user"],
            password=self.connection_info["password"],
        )

    @property
    def _attributes_list(self):
        return ", ".join([a["name"] for a in self.table_configuration.attributes])

    @property
    def _pk_list(self):
        return ", ".join(f"{k} = %s" for k in self.table_configuration.primary_key)

    def _pk_values(self, key):
        return [key[k] for k in self.table_configuration.primary_key]

    @property
    def _table_name(self):
        schema = self.table_configuration.table_schema if self.table_configuration.table_schema else "user_schema"
        return f"{schema}.{self.table_configuration.table_name}"

    def _convert_attribute(self, name, val):
        if name not in self.attributes_by_name:
            raise InputError(f"Invalid attribute {name}")
        type = self.attributes_by_name[name]["type"]
        if type == "int":
            return int(val)
        else:
            return val

    def _with_cursor(self, func):
        with self.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                return func(cur)

    def get_item(self, key):
        self._validate_primary_key(key)
        sql = f"SELECT {self._attributes_list} FROM {self._table_name} WHERE {self._pk_list};"

        def get(cur):
            cur.execute(sql, self._pk_values(key))
            record = cur.fetchone()
            return dict(record) if record is not None else None
        return self._with_cursor(get)

    def put_item(self, values, before_put=None):
        added_items = self.put_items([values], before_put)
        return added_items[0] if len(added_items) == 1 else None

    def put_items(self, values_batch, before_put=None):
        if not values_batch:
            raise InputError("Missing values")
        if len(values_batch) > TableBase.MAX_BATCH:
            raise InputError(f"Cannot add more than {TableBase.MAX_BATCH} records at once")

        for i, values in enumerate(values_batch):
            if before_put:
                before_put(values)

            for a in self.table_configuration.auto_generated_attributes:
                values[a] = generate_id()

        columns_to_insert = [k for k in values_batch[0] if k in self.attributes_by_name]
        columns_list = ", ".join(columns_to_insert)
        params_string = "(" + ", ".join(["%s" for c in columns_to_insert]) + ")"

        # separate loop because we want to populate auto-generated cols first,
        # before ensuring same columns across records
        for i, values in enumerate(values_batch):
            if [f["name"] for f in self.table_configuration.required_attributes if f["name"] not in values]:
                raise InputError("Missing required fields")
            for f in values:
                if f in self.attributes_by_name and f not in columns_to_insert:
                    raise InputError(f"Record {i} is missing '{f}'' which is present in record 0; all records "
                                     "must have same columns when batching")
        params = []
        multiple_record_params_string = []
        for values in values_batch:
            params += [values[k] for k in values if k in self.attributes_by_name]
            multiple_record_params_string.append(params_string)
        multiple_record_params_string = ", ".join(multiple_record_params_string)
        sql = f"INSERT INTO {self._table_name} ({columns_list}) VALUES {multiple_record_params_string}"

        def insert(cur):
            cur.execute(sql, params)
        self._with_cursor(insert)
        return values_batch

    def delete_item(self, key):
        deleted = self.delete_items([key])
        return deleted[0] if deleted else None

    def delete_items(self, keys_batch):
        if not keys_batch:
            raise InputError("Missing keys_batch")
        if len(keys_batch) > TableBase.MAX_BATCH:
            raise InputError(f"Cannot delete more than {TableBase.MAX_BATCH} records at once")

        params = []
        for key in keys_batch:
            self._validate_primary_key(key)
            params += self._pk_values(key)

        multiple_key_sql = " OR ".join([f"({self._pk_list})" for key in keys_batch])
        sql = f"DELETE FROM {self._table_name} WHERE {multiple_key_sql};"

        def delete(cur):
            cur.execute(sql, params)
        return self._with_cursor(delete)

    def update_item(self, key, values):
        self._validate_primary_key(key)
        value_keys_to_use = [k for k in values if k in self.attributes_by_name]
        if not value_keys_to_use:
            raise InputError("No valid update values provided")
        set_clause = ", ".join([f"{k} = %s" for k in value_keys_to_use])
        update_values = [values[k] for k in value_keys_to_use]
        sql = f"UPDATE {self._table_name} SET {set_clause} WHERE {self._pk_list}"

        def update(cur):
            cur.execute(sql, update_values + self._pk_values(key))
        self._with_cursor(update)
        return values

    def query(self, key, index=None):
        where = " AND ".join([f"{k} = %s" for k in key if k in self.attributes_by_name])
        params = [key[k] for k in key if k in self.attributes_by_name]
        if where:
            where = f"WHERE {where}"
        else:
            raise InputError("No valid query keys provided")

        sql = f"SELECT {self._attributes_list} FROM {self._table_name} {where}"

        def get(cur):
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        return self._with_cursor(get)

    def scan(self, offset=None):
        if offset is None:
            offset = 0
        sql = f"SELECT {self._attributes_list} FROM {self._table_name} LIMIT 1000 OFFSET {offset}"

        def get(cur):
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]
        return self._with_cursor(get)

    @staticmethod
    def attribute_to_postgres_sql(a, type_change=False):
        postgres_type = {
            "string": "text",
            "int": "numeric",
            "bool": "boolean",
        }.get(a.get("type", "string"), "text")
        name = a["name"]
        nullable = " NOT NULL" if a.get("required", False) else ""
        type_change_sql = "TYPE " if type_change else ""
        return f"{name} {type_change_sql}{postgres_type}{nullable}"

    @staticmethod
    def postgres_type_to_attribute_type(t):
        return {
            "int": "int",
            "integer": "int",
            "smallint": "int",
            "bigint": "int",
            "int2": "int",
            "int4": "int",
            "int8": "int",
            "real": "int",
            "float4": "int",
            "float8": "int",
            "double precision": "int",
            "numeric": "int",
            "boolean": "bool",
            "bool": "bool",
            "char": "string",
            "character": "string",
            "character varying": "string",
            "text": "string",
        }.get(t, "string")

    @property
    def create_table_sql(self):
        pk_sql = ", ".join(self.table_configuration.primary_key)
        pk_sql = f", PRIMARY KEY ({pk_sql})" if self.table_configuration.primary_key else ""
        columns_sql = ", \n".join([PostgresTable.attribute_to_postgres_sql(a)
                                   for a in self.table_configuration.attributes])
        statements = [f"CREATE TABLE {self._table_name} ({columns_sql}{pk_sql});"]
        if self.table_configuration.indexes:
            for ind, columns in self.table_configuration.indexes.items():
                if not columns:
                    raise InputError(f"No columns for index {ind}")
                index_columns = ", ".join(columns)
                statements.append(f"CREATE INDEX {self.table_configuration.table_name}_{ind} "
                                  f"ON {self._table_name} ({index_columns});")
        return statements

    @property
    def drop_table_sql(self):
        return f"DROP TABLE IF EXISTS {self._table_name};"

    def modify_table(self, changes):
        remove_columns = [f"DROP COLUMN {a['name']}" for a in changes.removed_attributes]
        add_columns = [f"ADD COLUMN {PostgresTable.attribute_to_postgres_sql(a)}" for a in changes.added_attributes]
        columns_changed_type = [a for a in changes.changed_attributes if
                                "original_type" in a and a["original_type"] != a["type"]]
        column_type_changes = [f"ALTER COLUMN {self.attribute_to_postgres_sql(a, True)}" for a in columns_changed_type]
        column_changes = ", ".join(remove_columns + add_columns + column_type_changes)
        sql = f"ALTER TABLE {self._table_name} {column_changes};"

        def modify_table(cur):
            columns_changed_name = [a for a in changes.changed_attributes
                                    if "original_name" in a and a["original_name"] != a["name"]]
            for a in columns_changed_name:
                cur.execute(f"ALTER TABLE {self._table_name} RENAME {a['original_name']} TO {a['name']};")
            if column_changes:
                cur.execute(sql)
        self._with_cursor(modify_table)

    def rename_table(self, new_name):
        def alter(cur):
            cur.execute(f"ALTER TABLE {self._table_name} RENAME TO {new_name};")
        self._with_cursor(alter)

    @property
    def is_sql_query_supported(self):
        return True

    def sql_query(self, sql, params):
        def execute_query(cur):
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        return self._with_cursor(execute_query)

    @property
    def is_paging_supported(self):
        return True
