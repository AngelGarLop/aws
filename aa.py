import boto3
from conexion import ConectorAWS
from boto3.dynamodb.conditions import Key

conector = ConectorAWS()
dynamodb = conector.conectarse()

print(dynamodb)
'''
print("1 -Crear al menos 3 tablas con dos atributos cada una ")
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

print("2 - Insertar 3 registros en cada tabla")
# Función para crear un registro
def put_item(table_name, item):
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    print(f"Item añadido a {table_name}: {item}")


# Crear tres registros en cada tabla
put_item('Usuario', {'ID': '1', 'Nombre': 'Paco', 'Edad': 25})
put_item('Usuario', {'ID': '2', 'Nombre': 'José', 'Edad': 58})
put_item('Usuario', {'ID': '3', 'Nombre': 'Antonio', 'Edad': 32})
put_item('Coche', {'ID': '1', 'Marca': 'Tesla', 'Duenno': 'Paco'})
put_item('Coche', {'ID': '2', 'Marca': 'Seat', 'Duenno': 'Luis'})
put_item('Coche', {'ID': '2', 'Marca': 'Ford', 'Duenno': 'Pedro'})
put_item('Semaforo', {'ID': '1', 'Ubicacion': 'Sevilla', 'Color': 'Rojo'})
put_item('Semaforo', {'ID': '2', 'Ubicacion': 'Malaga', 'Color': 'Amarillo'})
put_item('Semaforo', {'ID': '3', 'Ubicacion': 'Córdoba', 'Color': 'Azul'})



# Función para obtener un registro
def get_item(table_name, key):
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key)
    item = response.get('Item')
    print(f"Item from {table_name}: {item}")
    return item

# Obtener un registro de cada tabla
get_item('Usuario', {'Nombre': 'Paco','Edad': 25})
get_item('Coche', {'Marca': 'Tesla', 'Duenno': 'Paco'})
get_item('Semaforo', {'Ubicacion': 'Malaga', 'Color': 'Amarillo'})


print("4 - Actualizar un registro de cada tabla")
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
update_item('Usuario', {'Nombre': 'Paco', 'Edad': 25}, "set ID = :ID", {':ID': 6})
update_item('Coche', {'Marca': 'Tesla', 'Duenno': 'Paco'}, "set ID = :ID", {':ID': 5})
update_item('Semaforo', {'Ubicacion': 'Sevilla', 'Color': 'Rojo'}, "set ID = :ID", {':ID': 7})


print("5 - Eliminar un registro de cada tabla")

# Función para eliminar un registro
def delete_item(table_name, key):
    table = dynamodb.Table(table_name)
    table.delete_item(Key=key)
    print(f"Item deleted from {table_name}: {key}")

# Eliminar un registro de cada tabla
delete_item('Usuario', {'Nombre': 'Paco', 'Edad': 25})
delete_item('Coche', {'Marca': 'Tesla', 'Duenno': 'Paco'})
delete_item('Semaforo', {'Ubicacion': 'Sevilla', 'Color': 'Rojo'})
print("6 - Obtener todos los registros de cada tabla")

# Obtener todos los registros de cada tabla
def scan_table(table_name):
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items')
    print(f"Items from {table_name}: {items}")
    return items

scan_table('Usuario')
scan_table('Coche')
scan_table('Semaforo')

'''
print("7 - Filtrar registros de cada tabla")
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
query_table('Usuario', Key('Nombre','Edad').eq('Paco') & Key('Edad').eq(25), {':Nombre': 'Paco', ':Edad': 25})

'''
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