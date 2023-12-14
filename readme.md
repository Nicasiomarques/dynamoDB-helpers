
# DynamoDB Helper

DynamoDB Helper is a set of Python functions designed to simplify interactions with Amazon DynamoDB using the boto3 library. It provides convenient methods for common operations on a DynamoDB table, such as inserting, updating, deleting, querying, and more.

## Table of Contents
1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Basic Usage](#basic-usage)
4. [Methods](#methods)
    - [Initialization](#initialization)
    - [Insert Item](#insert-item)
    - [Get Item](#get-item)
    - [Update Item](#update-item)
    - [Delete Item](#delete-item)
    - [Scan Table](#scan-table)
    - [Query Table](#query-table)
    - [Batch Write Items](#batch-write-items)
    - [Create Global Secondary Index](#create-global-secondary-index)
5. [Error Handling](#error-handling)
6. [Customization](#customization)
7. [Logging](#logging)
8. [Contributing](#contributing)
9. [License](#license)

## Installation

Before using DynamoDB Helper, ensure that boto3 is installed. You can install it using the following command:

```bash
pip install boto3
```

Additionally, configure your AWS credentials either through the `~/.aws/credentials` file or using environment variables.

## Configuration

DynamoDB Helper is configured by providing the table name, primary key, and optional parameters during initialization. 

```python
from dynamodb_helper import DynamoDBHelper

# Replace 'MyTable' and 'ID' with the appropriate values for your table
dynamodb_helper = DynamoDBHelper(table_name='MyTable', primary_key='ID')
```

## Basic Usage

1. **Insert Item:**

   ```python
   item = {'ID': {'S': '1'}, 'Name': {'S': 'Example'}}
   dynamodb_helper.put_item(item)
   ```

2. **Get Item:**

   ```python
   result = dynamodb_helper.get_item('1')
   print(result)
   ```

3. **Update Item:**

   ```python
   update_expression = "SET #attrName = :attrValue"
   expression_attribute_values = {":attrValue": {'S': 'UpdatedValue'}}
   updated_attributes = dynamodb_helper.update_item('1', update_expression, {'#attrName': 'Name'})
   print(updated_attributes)
   ```

4. **Delete Item:**

   ```python
   dynamodb_helper.delete_item('1')
   ```

5. **Scan Table:**

   ```python
   all_items = dynamodb_helper.scan_table()
   print(all_items)
   ```

6. **Query Table:**

   ```python
   query_result = dynamodb_helper.query_table('1')
   print(query_result)
   ```

7. **Batch Write Items:**

   ```python
   batch_items = [{'ID': {'S': '2'}, 'Name': {'S': 'Item2'}}, {'ID': {'S': '3'}, 'Name': {'S': 'Item3'}}]
   dynamodb_helper.batch_write_items(batch_items)
   ```

8. **Create Global Secondary Index:**

   ```python
   index_name = 'MyGSI'
   key_schema = [('Name', 'S')]
   dynamodb_helper.create_global_secondary_index(index_name, key_schema)
   ```

## Methods

### Initialization

```python
dynamodb_helper = DynamoDBHelper(table_name, primary_key, primary_key_type='S', read_capacity=5, write_capacity=5)
```

- `table_name`: The name of the DynamoDB table.
- `primary_key`: The primary key attribute of the table.
- `primary_key_type`: The data type of the primary key (default is 'S' for string).
- `read_capacity`: Read capacity units for the provisioned throughput (default is 5).
- `write_capacity`: Write capacity units for the provisioned throughput (default is 5).

### Insert Item

```python
item = {'ID': {'S': '1'}, 'Name': {'S': 'Example'}}
dynamodb_helper.put_item(item)
```

Inserts an item into the DynamoDB table.

### Get Item

```python
result = dynamodb_helper.get_item('1')
print(result)
```

Retrieves an item from the DynamoDB table based on the provided primary key value.

### Update Item

```python
update_expression = "SET #attrName = :attrValue"
expression_attribute_values = {":attrValue": {'S': 'UpdatedValue'}}
updated_attributes = dynamodb_helper.update_item('1', update_expression, {'#attrName': 'Name'})
print(updated_attributes)
```

Updates an item in the DynamoDB table using the provided update expression and attribute values.

### Delete Item

```python
dynamodb_helper.delete_item('1')
```

Deletes an item from the DynamoDB table based on the provided primary key value.

### Scan Table

```python
all_items = dynamodb_helper.scan_table()
print(all_items)
```

Scans the entire DynamoDB table and returns all items.

### Query Table

```python
query_result = dynamodb_helper.query_table('1')
print(query_result)
```

Queries the DynamoDB table based on the provided primary key value.

### Batch Write Items

```python
batch_items = [{'ID': {'S': '2'}, 'Name': {'S': 'Item2'}}, {'ID': {'S': '3'}, 'Name': {'S': 'Item3'}}]
dynamodb_helper.batch_write_items(batch_items)
```

Writes multiple items to the DynamoDB table in a batch.

### Create Global Secondary Index

```python
index_name = 'MyGSI'
key_schema = [('Name', 'S')]
dynamodb_helper.create_global_secondary_index(index_name, key_schema)
```

Creates a global secondary index on the DynamoDB table.

## Error Handling

Error handling is implemented to capture and print relevant error messages during DynamoDB operations. However, it's important to customize error handling based on your application's specific requirements.

## Customization

Feel free to customize the provided code to suit the specific needs of your project. This may include additional functionalities or modifications to the existing functions.

## Logging

The code includes basic logging statements for debugging purposes. For production use, consider integrating a more robust logging system.

## Contributing

If you encounter issues or have suggestions for improvements, please open an issue or submit a pull request. Contributions are welcome!

## License

This project is licensed under the [MIT License](LICENSE).
