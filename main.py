import boto3
from botocore.exceptions import ClientError

class DynamoDBHelper:
	def __init__(self, table_name, primary_key, primary_key_type='S', read_capacity=5, write_capacity=5):
		self.table_name = table_name
		self.primary_key = primary_key
		self.primary_key_type = primary_key_type
		self.read_capacity = read_capacity
		self.write_capacity = write_capacity
		self.dynamodb = boto3.client('dynamodb')
		self._create_table()

	def _create_table(self):
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

	def _handle_error(self, operation, error):
		print(f"Error during {operation}: {error}")

	def put_item(self, item):
		try:
			self.dynamodb.put_item(
				TableName=self.table_name,
				Item=item
			)
			print("Item added successfully.")
		except ClientError as e:
			self._handle_error('put_item', e)

	def get_item(self, key_value):
		try:
			response = self.dynamodb.get_item(
				TableName=self.table_name,
				Key={self.primary_key: key_value}
			)
			return response.get('Item')
		except ClientError as e:
			self._handle_error('get_item', e)
			return None

	def update_item(self, key_value, update_expression, expression_attribute_values):
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

	def delete_item(self, key_value):
		try:
			self.dynamodb.delete_item(
				TableName=self.table_name,
				Key={self.primary_key: key_value}
			)
			print("Item deleted successfully.")
		except ClientError as e:
			self._handle_error('delete_item', e)

	def scan_table(self):
		try:
			response = self.dynamodb.scan(TableName=self.table_name)
			return response.get('Items')
		except ClientError as e:
			self._handle_error('scan_table', e)
			return None

	def query_table(self, key_value):
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

	def batch_write_items(self, items):
		try:
			with self.dynamodb.batch_writer() as batch:
				for item in items:
					batch.put_item(Item=item)
			print("Items written successfully in batch.")
		except ClientError as e:
			self._handle_error('batch_write_items', e)

	def create_global_secondary_index(self, index_name, key_schema, projection_type='ALL'):
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
