import cx_Oracle

# Datos de conexión
user = 'INFOCENT'
password = 'M4NZ4N1LL4'
#db_url = "INFOCENT/M4NZ4N1LL4@192.168.254.201:1521/SPITEST"

db_url = 'INFOCENT/M4NZ4N1LL4@192.168.254.201:1521/spitest'

# URL	jdbc:oracle:thin:@//192.168.254.201:1521/spitest

# Crear una conexión a la base de datos
connection = cx_Oracle.connect(user, password, db_url)

cursor = connection.cursor()

# Ejecutar una consulta SQL
sql_query = 'SELECT * FROM INFOCEN.EO_PERSONA'
cursor.execute(sql_query)

# Obtener resultados
results = cursor.fetchall()

# Procesar los resultados
for row in results:
    print(row)

cursor.close()
connection.close()
