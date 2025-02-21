#import cx_Oracle
from sqlalchemy import create_engine
import requests
import psycopg2
from datetime import datetime

import json
import pandas as pd

from dotenv import load_dotenv
# Carga las variables de entorno desde el archivo .env
load_dotenv()
import os



postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")

#credenciales postgresql
#dbnamePg = "spibuk"
#userPg = "postgres"
#passwordPg = "Q84Z7zQ2kR0WamnV4r6RLpWYhdD8JwDX"
#hostPg = "64.225.104.69"    # Cambia esto al host de tu base de datos
#portPg = "5432"             # Puerto predeterminado de PostgreSQL


try:
    connectionPg = psycopg2.connect(
        dbname=postgre_service,
        user=postgre_user,
        password=postgre_pass,
        host=postgre_host,
        port=postgre_port
    )
    #connectionPg = psycopg2.connect(
    #    dbname=dbnamePg,
    #    user=userPg,
    #    password=passwordPg,
    #    host=hostPg,
    #    port=portPg
    #)
    print("Conexión exitosa a PostgreSQL")
    engine = create_engine(f'postgresql://{postgre_user}:{postgre_pass}@{postgre_host}:{postgre_port}/{postgre_service}')
    
    # Crear un cursor
    cursor_email = connectionPg.cursor()
    cursor_ficha = connectionPg.cursor()

    cursor_email.execute("SELECT * FROM information_schema.tables WHERE table_name = 'workflow_api_email'")
    cursor_ficha.execute("SELECT * FROM information_schema.tables WHERE table_name = 'workflow_api_ficha'")

    # Verificar si se encontraron resultados
    if cursor_email.fetchone():
        # La tabla existe, puedes eliminar los registros
        cursor_email.execute("DELETE FROM workflow_api_email")
        connectionPg.commit()
        print("elimima workflow_api_email")

    if cursor_ficha.fetchone():
        # La tabla existe, puedes eliminar los registros
        cursor_ficha.execute("DELETE FROM workflow_api_ficha")
        connectionPg.commit()
        print("elimima workflow_api_ficha")


    print("empieza carga de api")
    ##*************************************** API BUK
    api_url = "https://alfonzorivas.buk.co/api/v1/workflow/alta/processes"
    #headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
    headers = {'auth_token': os.getenv('AUTH_TOKEN')}
    responseEmpleado = requests.get(api_url, headers=headers)
    print("carga de api OK")

    if responseEmpleado.status_code == 200:
        dataEmpleado = responseEmpleado.json()
        total_pages = dataEmpleado["pagination"]["total_pages"]
        for page in range(1, total_pages + 1):
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!pagina:',page)
            # Obtener la página actual
            url = f"{api_url}?page={page}"
            #headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
            headers = {'auth_token': os.getenv('AUTH_TOKEN')}
            response = requests.get(url, headers=headers)
            data = response.json()["data"]
            #print(data)

            selected_data_email = []
            selected_data_ficha = []
            for item in data:
                for operation in item["operations"]:
                    if operation["title"] == "ETAPA 1. GESTIÓN de ingreso":
                        # Si la operación está completada, agregar el elemento a la lista selected_data
                        selected_data_email.append({
                            "id": item["id"],
                            "title": item["title"],
                            "kind": item["kind"],
                            "document_number": item["employee"]["document_number"],
                            "rut": item["employee"]["rut"],
                            "first_name": item["employee"]["first_name"],
                            "last_name": item["employee"]["last_name"],
                            "segundo_apellido": item["employee"]["segundo_apellido"],
                            "start_date": item["employee"]["start_date"],
                            "operations": operation["title"],  
                            "completed_at": operation["completed_at"],
                            "completed_by_document_number": operation["completed_by"]["document_number"],
                            "completed_by_document_type": operation["completed_by"]["document_type"],
                            "completed_by_rut": operation["completed_by"]["rut"],
                            "completed_by_first_name": operation["completed_by"]["first_name"],
                            "completed_by_last_name": operation["completed_by"]["last_name"],
                            "completed_by_segundo_apellido": operation["completed_by"]["segundo_apellido"],
                            "completed_by_email": operation["completed_by"]["email"],
                            "status": item["status"],
                            "created_at": item["created_at"],
                            "created_by_document_number": item["created_by"]["document_number"],
                            "created_by_document_type": item["created_by"]["document_type"],
                            "created_by_rut": item["created_by"]["rut"],
                            "created_by_first_name": item["created_by"]["first_name"],
                            "created_by_last_name": item["created_by"]["last_name"],
                            "created_by_segundo_apellido": item["created_by"]["segundo_apellido"],
                            "created_by_email": item["created_by"]["email"],
                        })
                        print('IF',item["title"])
                        print('operations',operation["title"])
                    if operation["title"] == "Notificación: Totalmente aprobado. Nómina.":
                        # Si la operación está completada, agregar el elemento a la lista selected_data
                        selected_data_ficha.append({
                            "id": item["id"],
                            "title": item["title"],
                            "kind": item["kind"],
                            "document_number": item["employee"]["document_number"],
                            "rut": item["employee"]["rut"],
                            "first_name": item["employee"]["first_name"],
                            "last_name": item["employee"]["last_name"],
                            "segundo_apellido": item["employee"]["segundo_apellido"],
                            "start_date": item["employee"]["start_date"],
                            "operations": operation["title"],  
                            "completed_at": operation["completed_at"],
                            "completed_by_document_number": operation["completed_by"]["document_number"],
                            "completed_by_document_type": operation["completed_by"]["document_type"],
                            "completed_by_rut": operation["completed_by"]["rut"],
                            "completed_by_first_name": operation["completed_by"]["first_name"],
                            "completed_by_last_name": operation["completed_by"]["last_name"],
                            "completed_by_segundo_apellido": operation["completed_by"]["segundo_apellido"],
                            "completed_by_email": operation["completed_by"]["email"],
                            "status": item["status"],
                            "created_at": item["created_at"],
                            "created_by_document_number": item["created_by"]["document_number"],
                            "created_by_document_type": item["created_by"]["document_type"],
                            "created_by_rut": item["created_by"]["rut"],
                            "created_by_first_name": item["created_by"]["first_name"],
                            "created_by_last_name": item["created_by"]["last_name"],
                            "created_by_segundo_apellido": item["created_by"]["segundo_apellido"],
                            "created_by_email": item["created_by"]["email"],
                        })
                    print('procesando')
                    #endif
                #endfor
                print('sale de for operation in item["operations"]:')
            #endfor
            print('sale de for item in data:')

            print('df inicia creado')
            df_email = pd.DataFrame(selected_data_email)
            df_ficha = pd.DataFrame(selected_data_ficha)
            print('df fin creado')
            #print(df)
            #df.to_sql('mi_tabla', con=connectionPg, if_exists='append', index=False)
            print('df.to_sql inicia creado')
            df_email.to_sql('workflow_api_email', engine, if_exists='append', index=False)
            df_ficha.to_sql('workflow_api_ficha', engine, if_exists='append', index=False)
            print('df.to_sql fin creado')

            print('inserta el dataframe en la tabla')

    print("Transacción exitosa")


    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    connectionPg.commit()
    connectionPg.close()
    

except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")

