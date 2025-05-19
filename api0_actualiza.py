"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
    Este programa actualiza la tabla empleados de PostgreSQL con los datos de la API de BUK.
    La tabla empleados de PostgreSQL se carga de forma automatica por un trigger configurado en los Webhooks de BUK.

"""

#import cx_Oracle
import requests
import psycopg2
from datetime import datetime


from dotenv import load_dotenv
# Carga las variables de entorno desde el archivo .env
load_dotenv()
import os



postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")


try:

    connectionPg = psycopg2.connect(
        dbname=postgre_service,
        user=postgre_user,
        password=postgre_pass,
        host=postgre_host,
        port=postgre_port
    )
    print("Conexión exitosa a PostgreSQL")
    cursorApiEmpleado = connectionPg.cursor()

    connectionPg.autocommit = True ####esto se coloca para probar. Pero es recomendable este en automatico para que registre el LOG

    sql_query = "SELECT * FROM empleados where name is null and status_process is null order by 7 desc" # AND event_type  ='employee_create'"
    #sql_query = "SELECT * FROM empleados where id=16749"
    cursorApiEmpleado.execute(sql_query)
    results = cursorApiEmpleado.fetchall()
    #print(results)

    employee_id=''
    for row in results:
        employee_id=row[0]
        transacction_id=row[6]

        ##*************************************** API BUK
        api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees/"+employee_id
        headers = {'auth_token': os.getenv('AUTH_TOKEN')}
        responseEmpleado = requests.get(api_url, headers=headers)

        if responseEmpleado.status_code == 200:
            dataEmpleado = responseEmpleado.json()
            Buk_COMPANY=dataEmpleado.get("data", []).get("current_job", {}).get("company_id")  #CODIGO COMPAÑIA BUK
            Buk_NUM_IDEN=(dataEmpleado.get("data", []).get("rut")).replace('.', '')[:20]
            Name=(dataEmpleado.get("data", []).get("full_name"))
            #print("Datos de la API obtenidos con éxito.",'dataEmpleado',Buk_COMPANY)
        else:
            Buk_NUM_IDEN=''
            Name=''
            print("Error al realizar la solicitud GET a la API. :", responseEmpleado.status_code)

        api_url = "https://alfonzorivas.buk.co/api/v1/colombia/companies"
        headers = {'auth_token': os.getenv('AUTH_TOKEN')}
        responseEmpresa = requests.get(api_url, headers=headers)
        if responseEmpresa.status_code == 200:
            dataEmpresa = responseEmpresa.json() 
            result = [item for item in dataEmpresa.get("data", []) if item.get("id") == Buk_COMPANY]
            if result:
                elemento_encontrado = result[0]
                ID_EMPRESA = elemento_encontrado.get("custom_attributes", {}).get("codigo_empresa") #CODIGO COMPAÑIA BUK-> SPI
                #print(f"El valor de 'codigo_empresa' es: {ID_EMPRESA,}")


                print(employee_id,transacction_id,Name, Buk_NUM_IDEN,ID_EMPRESA)
                consulta = "UPDATE empleados set name=%s,ci=%s,company=%s where id=%s "
                cursorApiEmpleado.execute(consulta, (Name, Buk_NUM_IDEN,ID_EMPRESA,transacction_id))

            else:
                ID_EMPRESA=''
                print("No se encontró Empresa.")

        else:
            print("Error al realizar la solicitud GET a la API. :", responseEmpresa.status_code)

        #print(employee_id,transacction_id,Name, Buk_NUM_IDEN,ID_EMPRESA)
        #consulta = "UPDATE empleados set name=%s,ci=%s,company=%s where id=%s "
        #cursorApiEmpleado.execute(consulta, (Name, Buk_NUM_IDEN,ID_EMPRESA,transacction_id))
       

    #ENDFOR




    connectionPg.commit()
    #connectionPg.rollback()

    print("Transacción terminada")


    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    cursorApiEmpleado.close()
    connectionPg.close()
    

except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")

