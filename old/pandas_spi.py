import pandas as pd
import cx_Oracle

username = 'INFOCENT'
password = 'M4NZ4N1LL4'
host = '192.168.254.201'
port = 1521
service_name = 'spitest'

dsn = cx_Oracle.makedsn(host, port, service_name)

try:
    ##*************************************** ORACLE SPI
    connection = cx_Oracle.connect(username, password, dsn)
    print("Conexión exitosa a Oracle SPI")
  
    sql_query = 'SELECT * FROM EO_PERSONA'
    df = pd.read_sql(sql_query, connection)


    connection.close()
    
    print(df)

except cx_Oracle.Error as error:
    print("Error al conectar a Oracle SPI:", error)

