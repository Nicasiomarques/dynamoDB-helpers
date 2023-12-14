import boto3
from botocore.exceptions import ClientError
from typing import Any, Dict, List, Optional, Union

class DynamoDBHelper:
    """
    Helper class for interacting with DynamoDB.
    """

    def __init__(self, table_name: str, primary_key: str, primary_key_type: str = 'S',
                 read_capacity: int = 5, write_capacity: int = 5) -> None:
        """
        Initializes the DynamoDBHelper instance.

        Args:
            table_name (str): Name of the DynamoDB table.
            primary_key (str): Name of the primary key.
            primary_key_type (str, optional): Type of the primary key. Defaults to 'S'.
            read_capacity (int, optional): Read capacity units for the table. Defaults to 5.
            write_capacity (int, optional): Write capacity units for the table. Defaults to 5.
        """
        self.table_name: str = table_name
        self.primary_key: str = primary_key
        self.primary_key_type: str = primary_key_type
        self.read_capacity: int = read_capacity
        self.write_capacity: int = write_capacity
        self.dynamodb: Any = boto3.client('dynamodb')
        self._create_table()

    def _create_table(self) -> None:
        """
        Creates the DynamoDB table with specified parameters.
        """
        try:
            self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': self.primary_key, 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': self.primary_key, 'AttributeType': self.primary_key_type}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': self.read_capacity,
                    'WriteCapacityUnits': self.write_capacity
                }
            ).wait_until_exists()
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceInUseException':
                raise
            else:
                print(f"Table {self.table_name} already exists.")

    def _handle_error(self, operation: str, error: Exception) -> None:
        """
        Handles errors during DynamoDB operations.

        Args:
            operation (str): Name of the operation.
            error (Exception): The exception that occurred.
        """
        print(f"Error during {operation}: {error}")

    def put_item(self, item: Dict[str, Any]) -> None:
        """
        Adds an item to the DynamoDB table.

        Args:
            item (Dict[str, Any]): The item to be added.
        """
        try:
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=item
            )
            print("Item added successfully.")
        except ClientError as e:
            self._handle_error('put_item', e)

    def get_item(self, key_value: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        Retrieves an item from the DynamoDB table.

        Args:
            key_value (Union[str, int]): The value of the primary key.

        Returns:
            Optional[Dict[str, Any]]: The retrieved item or None if not found.
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={self.primary_key: key_value}
            )
            return response.get('Item')
        except ClientError as e:
            self._handle_error('get_item', e)
            return None

    def update_item(self, key_value: Union[str, int], update_expression: str,
                    expression_attribute_values: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Updates an item in the DynamoDB table.

        Args:
            key_value (Union[str, int]): The value of the primary key.
            update_expression (str): The update expression for modifying attributes.
            expression_attribute_values (Dict[str, Any]): Attribute values to be substituted in the expression.

        Returns:
            Optional[Dict[str, Any]]: The updated attributes or None if the update fails.
        """
        try:
            response = self.dynamodb.update_item(
                TableName=self.table_name,
                Key={self.primary_key: key_value},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW"
            )
            return response.get('Attributes')
        except ClientError as e:
            self._handle_error('update_item', e)
            return None

    def delete_item(self, key_value: Union[str, int]) -> None:
        """
        Deletes an item from the DynamoDB table.

        Args:
            key_value (Union[str, int]): The value of the primary key.
        """
        try:
            self.dynamodb.delete_item(
                TableName=self.table_name,
                Key={self.primary_key: key_value}
            )
            print("Item deleted successfully.")
        except ClientError as e:
            self._handle_error('delete_item', e)

    def scan_table(self) -> Optional[List[Dict[str, Any]]]:
        """
        Scans the entire DynamoDB table.

        Returns:
            Optional[List[Dict[str, Any]]]: List of items in the table or None if the scan fails.
        """
        try:
            response = self.dynamodb.scan(TableName=self.table_name)
            return response.get('Items')
        except ClientError as e:
            self._handle_error('scan_table', e)
            return None

    def query_table(self, key_value: Union[str, int]) -> Optional[List[Dict[str, Any]]]:
        """
        Queries the DynamoDB table based on the primary key.

        Args:
            key_value (Union[str, int]): The value of the primary key.

        Returns:
            Optional[List[Dict[str, Any]]]: List of items matching the query or None if the query fails.
        """
        try:
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression=f"{self.primary_key} = :value",
                ExpressionAttributeValues={":value": key_value}
            )
            return response.get('Items')
        except ClientError as e:
            self._handle_error('query_table', e)
            return None

    def batch_write_items(self, items: List[Dict[str, Any]]) -> None:
        """
        Writes a batch of items to the DynamoDB table.

        Args:
            items (List[Dict[str, Any]]): List of items to be written in the batch.
        """
        try:
            with self.dynamodb.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            print("Items written successfully in batch.")
        except ClientError as e:
            self._handle_error('batch_write_items', e)

    def create_global_secondary_index(self, index_name: str, key_schema: List[Tuple[str, str]],
                                      projection_type: str = 'ALL') -> None:
        """
        Creates a global secondary index on the DynamoDB table.

        Args:
            index_name (str): Name of the global secondary index.
            key_schema (List[Tuple[str, str]]): List of tuples representing the key schema.
            projection_type (str, optional): Type of projection for the index. Defaults to 'ALL'.
        """
        try:
            self.dynamodb.update_table(
                TableName=self.table_name,
                AttributeDefinitions=[{'AttributeName': key, 'AttributeType': type} for key, type in key_schema],
                GlobalSecondaryIndexUpdates=[{
                    'Create': {
                        'IndexName': index_name,
                        'KeySchema': [{'AttributeName': key, 'KeyType': 'HASH'} for key, _ in key_schema],
                        'Projection': {'ProjectionType': projection_type},
                        'ProvisionedThroughput': {'ReadCapacityUnits': self.read_capacity, 'WriteCapacityUnits': self.write_capacity}
                    }
                }]
            )
            print(f"Global secondary index {index_name} created successfully.")
        except ClientError as e:
            self._handle_error('create_global_secondary_index', e)


# Example Usage:

# Initialize DynamoDB Helper
table_name = 'MyTable'
primary_key = 'ID'
dynamodb_helper = DynamoDBHelper(table_name, primary_key)

# Insert Item
item = {'ID': {'S': '1'}, 'Name': {'S': 'Example'}}
dynamodb_helper.put_item(item)

# Get Item
result = dynamodb_helper.get_item('1')
print(result)

# Update Item
update_expression = "SET #attrName = :attrValue"
expression_attribute_values = {":attrValue": {'S': 'UpdatedValue'}}
updated_attributes = dynamodb_helper.update_item('1', update_expression, {'#attrName': 'Name'})
print(updated_attributes)

# Delete Item
dynamodb_helper.delete_item('1')

# Scan Table
all_items = dynamodb_helper.scan_table()
print(all_items)

# Query Table
query_result = dynamodb_helper.query_table('1')
print(query_result)

# Batch Write Items
batch_items = [{'ID': {'S': '2'}, 'Name': {'S': 'Item2'}}, {'ID': {'S': '3'}, 'Name': {'S': 'Item3'}}]
dynamodb_helper.batch_write_items(batch_items)

# Create Global Secondary Index
index_name = 'MyGSI'
key_schema = [('Name', 'S')]
dynamodb_helper.create_global_secondary_index(index_name, key_schema)
