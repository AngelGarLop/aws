import boto3
from conexion import ConectorAWS
from boto3.dynamodb.conditions import Key,Attr
import time

conector = ConectorAWS()
dynamodb = conector.conectarse()
dynamodb_client = conector.conectarse_client()

print(dynamodb)
print(dynamodb_client)

print("1 -Crear al menos 3 tablas con dos atributos cada una ")
def create_table(table_name, key_schema, attribute_definitions, provisioned_throughput):
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=provisioned_throughput
    )
    table.wait_until_exists()
    print(f"tabla {table_name} creada satisfactoriamente.")

create_table('Usuario', 
             [{'AttributeName': 'Nombre', 'KeyType': 'HASH'}, {"AttributeName": "Edad", "KeyType": "RANGE"}], 
             [{'AttributeName': 'Nombre', 'AttributeType': 'S'}, {'AttributeName': 'Edad', 'AttributeType': 'N'}], 
             {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})

create_table('Coche', 
             [{'AttributeName': 'Marca', 'KeyType': 'HASH'}, {"AttributeName": "Duenno", "KeyType": "RANGE"}], 
             [{'AttributeName': 'Marca', 'AttributeType': 'S'}, {'AttributeName': 'Duenno', 'AttributeType': 'S'}], 
             {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})

create_table('Semaforo', 
             [{'AttributeName': 'Ubicacion', 'KeyType': 'HASH'}, {"AttributeName": "Color", "KeyType": "RANGE"}], 
             [{'AttributeName': 'Ubicacion', 'AttributeType': 'S'}, {'AttributeName': 'Color', 'AttributeType': 'S'}], 
             {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})

print("2 - Insertar 3 registros en cada tabla")
def put_item(table_name, item):
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    print(f"Registro añadido a la tabla {table_name}: {item}")

put_item('Usuario', {'ID': '1', 'Nombre': 'Paco', 'Edad': 25})
put_item('Usuario', {'ID': '2', 'Nombre': 'José', 'Edad': 58})
put_item('Usuario', {'ID': '3', 'Nombre': 'Antonio', 'Edad': 32})
put_item('Coche', {'ID': '1', 'Marca': 'Tesla', 'Duenno': 'Paco'})
put_item('Coche', {'ID': '2', 'Marca': 'Seat', 'Duenno': 'Luis'})
put_item('Coche', {'ID': '2', 'Marca': 'Ford', 'Duenno': 'Pedro'})
put_item('Semaforo', {'ID': '1', 'Ubicacion': 'Sevilla', 'Color': 'Rojo'})
put_item('Semaforo', {'ID': '2', 'Ubicacion': 'Malaga', 'Color': 'Amarillo'})
put_item('Semaforo', {'ID': '3', 'Ubicacion': 'Córdoba', 'Color': 'Azul'})

print("3 - Obtener un registro de cada tabla")
def get_item(table_name, key):
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key)
    item = response.get('Item')
    print(f"Registro de la tabla {table_name}: {item}")
    return item

get_item('Usuario', {'Nombre': 'Paco','Edad': 25})
get_item('Coche', {'Marca': 'Tesla', 'Duenno': 'Paco'})
get_item('Semaforo', {'Ubicacion': 'Malaga', 'Color': 'Amarillo'})


print("4 - Actualizar un registro de cada tabla")
def update_item(table_name, key, update_expression, expression_attribute_values):
    table = dynamodb.Table(table_name)
    table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    print(f"Registro actualizado en la tabala {table_name}: {key}")

update_item('Usuario', {'Nombre': 'Paco', 'Edad': 25}, "set ID = :ID", {':ID': 6})
update_item('Coche', {'Marca': 'Tesla', 'Duenno': 'Paco'}, "set ID = :ID", {':ID': 5})
update_item('Semaforo', {'Ubicacion': 'Sevilla', 'Color': 'Rojo'}, "set ID = :ID", {':ID': 7})


print("5 - Eliminar un registro de cada tabla")
def delete_item(table_name, key):
    table = dynamodb.Table(table_name)
    table.delete_item(Key=key)
    print(f"Registro elimininado de la tabala {table_name}: {key}")

delete_item('Usuario', {'Nombre': 'Paco', 'Edad': 25})
delete_item('Coche', {'Marca': 'Tesla', 'Duenno': 'Paco'})
delete_item('Semaforo', {'Ubicacion': 'Sevilla', 'Color': 'Rojo'})

print("6 - Obtener todos los registros de cada tabla")
def scan_table(table_name):
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items')
    print(f"Registros de la tabla {table_name}: {items}")
    return items

scan_table('Usuario')
scan_table('Coche')
scan_table('Semaforo')


print("7 - Filtrar registros de cada tabla")
def query_table(table_name, key_condition_expression):
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=key_condition_expression,
       
    )
    items = response.get('Items')
    print(f"Registros consultados de la tabla {table_name}: {items}")
    return items

query_table('Usuario', Key('Nombre').eq('José'))
query_table('Coche', Key('Marca').eq('Ford'))
query_table('Semaforo', Key('Ubicacion').eq('Córdoba'))

print("8 - Realizar una eliminación condicional de cada tabla")
def conditional_delete_item(table_name, key, condition_expression, ):
    table = dynamodb.Table(table_name)
    table.delete_item(
        Key=key,
        ConditionExpression=condition_expression,
    )
    print(f"Registro condicionalmente eliminiado de la tabla {table_name}: {key}")

conditional_delete_item('Usuario', {'Nombre': 'José', 'Edad': 58}, "attribute_exists(Nombre)")
conditional_delete_item('Coche', {'Marca': 'Ford', 'Duenno': 'Pedro'}, 'attribute_exists(Marca)')
conditional_delete_item('Semaforo', {'Ubicacion': 'Córdoba', 'Color': 'Azul'}, 'attribute_exists(Ubicacion)')

print("9 - Obtener un conjunto de datos a través de varios filtros aplicado en cada tabla")
def query_table(table_name, key_condition_expression, filter_expression=None):
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=key_condition_expression,
        FilterExpression=filter_expression,
    )
    items = response.get('Items')
    print(f"Registros consultados de la tabla {table_name}: {items}")
    return items

query_table('Usuario', Key('Nombre').eq('Antonio'), filter_expression=Attr('ID').eq('3'))
query_table( 'Coche', Key('Marca').eq('Seat'), filter_expression=Attr('ID').eq('2'))
query_table('Semaforo', Key('Ubicacion').eq('Malaga'),filter_expression=Attr('ID').eq('2'))

print("10 - Utilizar PartiQL statement en cada tabla")

def execute_partiql(statement):
    response = dynamodb_client.execute_statement(Statement=statement)
    print(response)
    return response

execute_partiql("SELECT * FROM Usuario WHERE ID = '3'")
execute_partiql("SELECT * FROM Coche WHERE ID = '2'")
execute_partiql("SELECT * FROM Semaforo WHERE ID = '2'")


print("11 - Crear un backup de todas las tablas")
def create_backup(table_name):
    response = dynamodb_client.create_backup(
        TableName=table_name,
        BackupName=f"{table_name}_backup"
    )
    print(f"Backup creado de la tabla {table_name}: {response}")

create_backup('Usuario')
create_backup('Coche')
create_backup('Semaforo')