import cx_Oracle

username = 'INFOCENT'
password = 'M4NZ4N1LL4'
host = '192.168.254.201'
port = 1521
service_name = 'spitest'

dsn = cx_Oracle.makedsn(host, port, service_name)

connection = cx_Oracle.connect(username, password, dsn)
print("Conexión exitosa a Oracle SPI")
  
cursor = connection.cursor()


# Texto a buscar
search_text = 'ETL'

# Columna específica a buscar
column_name = 'USRCRE'
FILTRO_OWNER='INFOCENT'

try:
    # Obtener la lista de todas las tablas y columnas
    cursor.execute("""
        SELECT table_name, column_name
        FROM all_tab_columns
        WHERE column_name = :column_name AND OWNER=:FILTRO_OWNER
    """, {'column_name': column_name,'FILTRO_OWNER':FILTRO_OWNER})

    # Recorrer las tablas y columnas
    for table_name, column_name in cursor.fetchall():
        #print('table_name',table_name)
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} LIKE '%' || :search_text || '%'"
        #query = f"SELECT COUNT(*) FROM NMM011_01112012 WHERE {column_name} LIKE '%' || :search_text || '%'"
        cursor.execute(query, {'search_text': search_text})
        result = cursor.fetchone()[0]

        if result > 0:
            print(f'Texto encontrado en {table_name}.{column_name}')

finally:
    # Cerrar el cursor y la conexión
    cursor.close()
    connection.close()