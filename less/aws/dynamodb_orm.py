import random

import boto3

class InputError(Exception):
    def __init__(self, message):
        self.message = message


CHARS_FOR_KEY = "ACDEFGHIJKLMNPQRSTUVWXYZabcdefghijkmnpqrsuvwxyz2345679"


def generate_id():
    return "".join(random.choices(CHARS_FOR_KEY, k=10))


class Table(object):
    def __init__(self, table_configuration):
        self.table_configuration = table_configuration
        self.attributes_by_name = {
            k["name"]: k for k in self.table_configuration.attributes
        }
        self.client = boto3.client('dynamodb')

    def _validate_primary_key(self, key):
        if not key:
            raise InputError("Missing key")
        for k in self.table_configuration.primary_key:
            if k not in key:
                raise InputError(f"Missing '{k}'")

    def translate_from_dynamodb_item(self, item, attributes_by_name=None):
        translated = {}
        if attributes_by_name is None:
            attributes_by_name = self.attributes_by_name
        for k in item:
            if k in attributes_by_name:
                attribute_type = attributes_by_name[k].get("type", "string")
                if attribute_type != "list":
                    translated[k] = item[k][Table.dynamodb_type(attribute_type)]
                else:
                    child_attributes = {
                        child["name"]: child for child in attributes_by_name[k]["attributes"]
                    }
                    translated[k] = [self.translate_from_dynamodb_item(child["M"], child_attributes) for child in item[k]["L"]]
        return translated

    def translate_to_dynamodb_item(self, item, attributes_by_name=None):
        translated = {}
        if attributes_by_name is None:
            attributes_by_name = self.attributes_by_name
        for k in item:
            if k in attributes_by_name:
                attribute_type = attributes_by_name[k].get("type", "string")
                if attribute_type != "list":
                    translated[k] = {
                        Table.dynamodb_type(attribute_type): item[k]
                    }
                else:
                    child_attributes = {
                        child["name"]: child for child in attributes_by_name[k]["attributes"]
                    }
                    translated[k] = {
                        "L": [
                            {
                                "M": self.translate_to_dynamodb_item(child, child_attributes)
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

    def query(self, key):
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
        response = self.client.query(
            TableName=self.table_configuration.table_name,
            KeyConditionExpression=key_condition,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names,
        )
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
