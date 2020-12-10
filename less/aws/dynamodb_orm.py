import boto3

from less.aws.table_base import InputError, generate_id, TableBase


class Table(TableBase):
    def __init__(self, table_configuration):
        self.table_configuration = table_configuration
        self.attributes_by_name = {
            k["name"]: k for k in self.table_configuration.attributes
        }
        self.client = boto3.client('dynamodb')

    MAX_DYNAMODB_BATCH = 25

    def translate_from_dynamodb_item(self, item, attributes_by_name=None):
        translated = {}
        if attributes_by_name is None:
            attributes_by_name = self.attributes_by_name
        for k in item:
            if k in attributes_by_name:
                attribute_type = attributes_by_name[k].get("type", "string")
                if attribute_type != "list":
                    v = item[k][Table.dynamodb_type(attribute_type)]
                    if attribute_type == "int":
                        v = float(v)
                    translated[k] = v
                else:
                    child_attributes = {
                        child["name"]: child for child in attributes_by_name[k]["attributes"]
                    }
                    translated[k] = [self.translate_from_dynamodb_item(child["M"], child_attributes) for child in item[k]["L"]]
        return translated

    def translate_to_dynamodb_item(self, item, attributes_by_name=None, key_prefix=""):
        translated = {}
        if attributes_by_name is None:
            attributes_by_name = self.attributes_by_name
        for k in item:
            if k in attributes_by_name:
                key_to_use = f"{key_prefix}{k}"
                attribute_type = attributes_by_name[k].get("type", "string")
                if attribute_type != "list":
                    translated[key_to_use] = {
                        Table.dynamodb_type(attribute_type): str(item[k]) if attribute_type == "int" else item[k]
                    }
                else:
                    child_attributes = {
                        child["name"]: child for child in attributes_by_name[k]["attributes"]
                    }
                    translated[key_to_use] = {
                        "L": [
                            {
                                "M": self.translate_to_dynamodb_item(
                                    child,
                                    child_attributes,
                                )
                            } for child in item[k]
                        ]
                    }
        return translated

    def _key_from_params(self, params):
        return {k: {"S": params[k]} for k in self.table_configuration.primary_key}

    def get_items(self, keys):
        for key in keys:
            self._validate_primary_key(key)
        response = self.client.batch_get_item(
            RequestItems={
                self.table_configuration.table_name: {
                    "Keys": [self._key_from_params(k) for k in keys]
                }
            }
        )
        if "Responses" not in response:
            return []
        items = response["Responses"]
        if self.table_configuration.table_name not in items:
            return []
        items = items[self.table_configuration.table_name]
        return [self.translate_from_dynamodb_item(item) for item in items]

    def get_item(self, key):
        items = self.get_items([key])
        return items[0] if items else None

    def delete_item(self, key):
        return self.delete_items([key])

    def delete_items(self, keys_batch):
        if len(keys_batch) > TableBase.MAX_BATCH:
            raise InputError(f"Cannot delete more than {TableBase.MAX_BATCH} records at once")

        for key in keys_batch:
            self._validate_primary_key(key)
        for i in range(0, len(keys_batch), Table.MAX_DYNAMODB_BATCH):
            chunk = keys_batch[i:i+Table.MAX_DYNAMODB_BATCH]
            self.client.batch_write_item(
                RequestItems={
                    self.table_configuration.table_name: [
                        {
                            "DeleteRequest": {
                                "Key": self._key_from_params(key)
                            }
                        } for key in chunk
                    ]
                }
            )
        return keys_batch

    def _query_multiple(self, keys, index=None):
        key_attribute = list(keys[0])[0]
        for key in keys:
            if not self.table_configuration.is_primary_key(key) and \
                    index and self.table_configuration.get_index_name(key) != index:
                raise InputError("Passed in key is not primary key and does not match requested index")
            if len(key) > 1:
                raise InputError("Only single attribute keys are supported for multiple value queries")
            if list(key)[0] != key_attribute:
                raise InputError("Invalid key: all values must be for same attribute")

        # We know that each key in keys has only one attribute

        key_condition_vals = ", ".join([f":{key_attribute}_{i}" for i, k in enumerate(keys)])
        key_condition = f"#{key_attribute} IN ({key_condition_vals})"
        expression_values = {
            f":{key_attribute}_{i}": {
                Table.dynamodb_type(self.attributes_by_name[key_attribute].get("type", "string")): k[key_attribute]
            } for i, k in enumerate(keys)
        }
        expression_names = {
            f"#{key_attribute}": key_attribute
        }
        kwargs = {
            "TableName": self.table_configuration.table_name,
            "FilterExpression": key_condition,
            "ExpressionAttributeValues": expression_values,
            "ExpressionAttributeNames": expression_names,
        }
        response = self.client.scan(**kwargs)
        return [self.translate_from_dynamodb_item(i) for i in response.get("Items", [])]

    def query(self, key, index=None):
        if isinstance(key, list):
            if len(key) > 1:
                return self._query_multiple(key, index)
            elif len(key) == 1:
                key = key[0]
            else:
                raise InputError("Empty key")

        if not self.table_configuration.is_primary_key(key) and \
           index and self.table_configuration.get_index_name(key) != index:
            raise InputError("Passed in key is not primary key and does not match requested index")
        key_items_to_use = key
        key_condition = " AND ".join([f"#{k} = :{k}" for k in key_items_to_use])
        expression_values = {
            f":{k}": {
                Table.dynamodb_type(self.attributes_by_name[k].get("type", "string")): key[k]
            } for k in key_items_to_use
        }
        expression_names = {
            f"#{k}": k for k in key_items_to_use
        }
        kwargs = {
            "TableName": self.table_configuration.table_name,
            "KeyConditionExpression": key_condition,
            "ExpressionAttributeValues": expression_values,
            "ExpressionAttributeNames": expression_names,
        }
        if index is not None:
            kwargs["IndexName"] = index
        response = self.client.query(**kwargs)
        return [self.translate_from_dynamodb_item(i) for i in response.get("Items", [])]

    def scan(self, offset=None):
        if offset:
            raise InputError("offset is not supported")
        response = self.client.scan(
            TableName=self.table_configuration.table_name,
            Limit=TableBase.MAX_BATCH
        )
        return [self.translate_from_dynamodb_item(i) for i in response.get("Items", [])]

    @staticmethod
    def dynamodb_type(type):
        return {
            "string": "S",
            "bool": "BOOL",
            "int": "N",
            "list": "L"
        }.get(type, "S")

    def put_item(self, values, before_put=None):
        values = self.put_items([values], before_put)
        return values[0] if len(values) == 1 else None

    def put_items(self, values_batch, before_put=None):
        if not values_batch:
            raise InputError("Missing values")
        if len(values_batch) > TableBase.MAX_BATCH:
            raise InputError(f"Cannot put more than {TableBase.MAX_BATCH} records at once")
        preprocessed_batch = []
        for rec in values_batch:
            item = rec
            if before_put:
                before_put(item)
            for a in self.table_configuration.auto_generated_attributes:
                item[a] = generate_id()
            if [f["name"] for f in self.table_configuration.required_attributes if f["name"] not in item]:
                raise InputError("Missing required fields")
            preprocessed_batch.append(item)
        for i in range(0, len(preprocessed_batch), Table.MAX_DYNAMODB_BATCH):
            chunk = preprocessed_batch[i:i+Table.MAX_DYNAMODB_BATCH]
            self.client.batch_write_item(
                RequestItems={
                    self.table_configuration.table_name: [
                        {
                            "PutRequest": {
                                "Item": self.translate_to_dynamodb_item(item)
                            }
                        } for item in chunk
                    ]
                }
            )
        return preprocessed_batch

    def update_item(self, key, values):
        self._validate_primary_key(key)
        expression_values = self.translate_to_dynamodb_item(values, None, ":")
        expression_attribute_names = {f"#{k}": k for k in values if k in self.attributes_by_name}
        update_expression = "SET " + ", ".join([f"#{k} = :{k}" for k in values if k in self.attributes_by_name])
        self.client.update_item(
            TableName=self.table_configuration.table_name,
            Key=self._key_from_params(key),
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_attribute_names,
        )
        return values

    def modify_table(self, changes):
        if changes.removed_attributes:
            raise InputError("Removing attributes is not supported for DynamoDB tables")
        if changes.changed_attributes:
            raise InputError("Modifying an attribute is not supported for DynamoDB tables")
