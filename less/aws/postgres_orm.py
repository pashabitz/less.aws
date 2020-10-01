import psycopg2
from psycopg2.extras import RealDictCursor

from less.aws.table_base import InputError, TableBase


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
        return f"user_schema.{self.table_configuration.table_name}"

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

    def delete_item(self, key):
        self._validate_primary_key(key)
        sql = f"DELETE FROM {self._table_name} WHERE {self._pk_list};"

        def delete(cur):
            cur.execute(sql, self._pk_values(key))
        return self._with_cursor(delete)

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

    def scan(self):
        sql = f"SELECT {self._attributes_list} FROM {self._table_name} LIMIT 1000"

        def get(cur):
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]
        return self._with_cursor(get)
