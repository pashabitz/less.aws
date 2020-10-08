import boto3

from less.aws.table_base import InputError, generate_id, TableBase


class Table(TableBase):
    def __init__(self, table_configuration):
        self.table_configuration = table_configuration
        self.attributes_by_name = {
            k["name"]: k for k in self.table_configuration.attributes
        }
        self.client = boto3.client('dynamodb')

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

    def get_item(self, key):
        self._validate_primary_key(key)
        response = self.client.get_item(
            TableName=self.table_configuration.table_name,
            Key=self._key_from_params(key)
        )
        item = response.get("Item", None)
        return self.translate_from_dynamodb_item(item) if item else None

    def delete_item(self, key):
        self._validate_primary_key(key)
        return self.client.delete_item(
            TableName=self.table_configuration.table_name,
            Key=self._key_from_params(key)
        )

    def query(self, key, index=None):
        key_items_to_use = [
            k for k in key if k in self.table_configuration.primary_key or
            k in self.table_configuration.index_attributes
        ]
        key_condition = " AND ".join([f"#{k} = :{k}" for k in key_items_to_use])
        expression_values = {
            f":{k}": {
                "S": key[k]
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

    def scan(self):
        response = self.client.scan(
            TableName=self.table_configuration.table_name,
            Limit=1000
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
        if not values:
            raise InputError("Missing values")
        if before_put:
            before_put(values)

        for a in self.table_configuration.auto_generated_attributes:
            values[a] = generate_id()

        if [f["name"] for f in self.table_configuration.required_attributes if f["name"] not in values]:
            raise InputError("Missing required fields")
        item = self.translate_to_dynamodb_item(values)
        self.client.put_item(
            TableName=self.table_configuration.table_name,
            Item=item
        )
        return values

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
