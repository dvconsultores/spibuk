import cx_Oracle
import requests

username = 'INFOCENT'
password = 'M4NZ4N1LL4'
host = '192.168.254.201'
port = 1521
service_name = 'spitest'

dsn = cx_Oracle.makedsn(host, port, service_name)

try:
    ##*************************************** ORACLE SPI
    #connection = cx_Oracle.connect(username, password, dsn)
    #print("Conexión exitosa a Oracle SPI")
  
    #cursor = connection.cursor()
    #sql_query = 'SELECT * FROM EO_PERSONA'
    #cursor.execute(sql_query)

    #results = cursor.fetchall()
 
    #for row in results:
    #    print(row)
    ##*************************************** API BUK
    api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees"
    headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        data = response.json() 
        print("Datos de la API obtenidos con éxito.",data)
    else:
        print("Error al realizar la solicitud GET a la API. :", response.status_code)
    #connection.close()
except cx_Oracle.Error as error:
    print("Error al conectar a Oracle SPI:", error)

