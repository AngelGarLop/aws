import boto3
from conexion import ConectorAWS

conector = ConectorAWS()
dynamodb = conector.conectarse()

print(dynamodb)
# Función para crear una tabla
def create_table(table_name, key_schema, attribute_definitions, provisioned_throughput):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=provisioned_throughput
    )
    #table.wait_until_exists()
    #print(f"Table {table_name} created successfully.")

# Crear tres tablas
create_table('Usuario', 
             [{'AttributeName': 'Nombre', 'KeyType': 'HASH'}, {"AttributeName": "Edad", "KeyType": "RANGE"}], 
             [{'AttributeName': 'Nombre', 'AttributeType': 'S'}, {'AttributeName': 'Edad', 'AttributeType': 'N'}], 
             {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

create_table('Coche', 
             [{'AttributeName': 'Marca', 'KeyType': 'HASH'}, {"AttributeName": "Duenno", "KeyType": "RANGE"}], 
             [{'AttributeName': 'Marca', 'AttributeType': 'S'}, {'AttributeName': 'Duenno', 'AttributeType': 'S'}], 
             {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

create_table('Semaforo', 
             [{'AttributeName': 'Ubicacion', 'KeyType': 'HASH'}, {"AttributeName": "Color", "KeyType": "RANGE"}], 
             [{'AttributeName': 'Ubicacion', 'AttributeType': 'S'}, {'AttributeName': 'Color', 'AttributeType': 'S'}], 
             {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})

'''
# Función para crear un registro
def put_item(table_name, item):
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    print(f"Item added to {table_name}: {item}")

# Crear tres registros en cada tabla
put_item('Tabla1', {'ID': '1', 'Atributo1': 'Valor1', 'Atributo2': 'Valor2'})
put_item('Tabla2', {'ID': '2', 'Atributo1': 'Valor1', 'Atributo2': 'Valor2'})
put_item('Tabla3', {'ID': '3', 'Atributo1': 'Valor1', 'Atributo2': 'Valor2'})

# Función para obtener un registro
def get_item(table_name, key):
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key)
    item = response.get('Item')
    print(f"Item from {table_name}: {item}")
    return item

# Obtener un registro de cada tabla
get_item('Tabla1', {'ID': '1'})
get_item('Tabla2', {'ID': '2'})
get_item('Tabla3', {'ID': '3'})

# Función para actualizar un registro
def update_item(table_name, key, update_expression, expression_attribute_values):
    table = dynamodb.Table(table_name)
    table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    print(f"Item updated in {table_name}: {key}")

# Actualizar un registro en cada tabla
update_item('Tabla1', {'ID': '1'}, "set Atributo1 = :val", {':val': 'NuevoValor1'})
update_item('Tabla2', {'ID': '2'}, "set Atributo1 = :val", {':val': 'NuevoValor1'})
update_item('Tabla3', {'ID': '3'}, "set Atributo1 = :val", {':val': 'NuevoValor1'})

# Función para eliminar un registro
def delete_item(table_name, key):
    table = dynamodb.Table(table_name)
    table.delete_item(Key=key)
    print(f"Item deleted from {table_name}: {key}")

# Eliminar un registro de cada tabla
delete_item('Tabla1', {'ID': '1'})
delete_item('Tabla2', {'ID': '2'})
delete_item('Tabla3', {'ID': '3'})

# Obtener todos los registros de cada tabla
def scan_table(table_name):
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items')
    print(f"Items from {table_name}: {items}")
    return items

scan_table('Tabla1')
scan_table('Tabla2')
scan_table('Tabla3')

# Filtrar registros de cada tabla
def query_table(table_name, key_condition_expression, expression_attribute_values):
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    items = response.get('Items')
    print(f"Queried items from {table_name}: {items}")
    return items

# Ejemplo de filtro
query_table('Tabla1', 'ID = :id', {':id': '1'})

# Eliminación condicional
def conditional_delete_item(table_name, key, condition_expression, expression_attribute_values):
    table = dynamodb.Table(table_name)
    table.delete_item(
        Key=key,
        ConditionExpression=condition_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    print(f"Conditionally deleted item from {table_name}: {key}")

# Ejemplo de eliminación condicional
conditional_delete_item('Tabla1', {'ID': '1'}, 'attribute_not_exists(Atributo1)', {':val': 'Valor1'})

# PartiQL statement
def execute_partiql(statement):
    client = boto3.client('dynamodb')
    response = client.execute_statement(Statement=statement)
    print(f"PartiQL statement executed: {statement}")
    return response

# Ejemplo de PartiQL
execute_partiql("SELECT * FROM Tabla1 WHERE ID = '1'")

# Crear un backup de todas las tablas
def create_backup(table_name):
    client = boto3.client('dynamodb')
    response = client.create_backup(
        TableName=table_name,
        BackupName=f"{table_name}_backup"
    )
    print(f"Backup created for {table_name}: {response}")

create_backup('Tabla1')
create_backup('Tabla2')
create_backup('Tabla3')

# Aquí se puede agregar la lógica para documentar las capturas y almacenar el código en un repositorio de git.

print("All functionalities executed successfully.")
'''