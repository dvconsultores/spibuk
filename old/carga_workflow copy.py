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
dbnamePg = "spibuk"
userPg = "postgres"
passwordPg = "Q84Z7zQ2kR0WamnV4r6RLpWYhdD8JwDX"
hostPg = "64.225.104.69"    # Cambia esto al host de tu base de datos
portPg = "5432"             # Puerto predeterminado de PostgreSQL


try:
    connectionPg = psycopg2.connect(
        dbname=dbnamePg,
        user=userPg,
        password=passwordPg,
        host=hostPg,
        port=portPg
    )
    print("Conexión exitosa a PostgreSQL")
    engine = create_engine(f'postgresql://{userPg}:{passwordPg}@{hostPg}:{portPg}/{dbnamePg}')
    
    # Crear un cursor
    cursor = connectionPg.cursor()

    # Consulta para verificar si la tabla existe
    cursor.execute("DROP TABLE IF EXISTS workflow_api")


    ##*************************************** API BUK
    api_url = "https://alfonzorivas.buk.co/api/v1/workflow/alta/processes"
    headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
    responseEmpleado = requests.get(api_url, headers=headers)

    if responseEmpleado.status_code == 200:
        dataEmpleado = responseEmpleado.json()
        total_pages = dataEmpleado["pagination"]["total_pages"]
        for page in range(1, total_pages + 1):
            # Obtener la página actual
            url = f"{api_url}?page={page}"
            headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
            response = requests.get(url, headers=headers)
            data = response.json()["data"]
            #print(data)

            selected_data = []
            for item in data:
                # ... (resto del código)

                # Validar si la operación "title" es igual a "completado"
                for operation in item["operations"]:
                    if operation["title"] == "completado":
                        # Si la operación está completada, agregar el elemento a la lista selected_data
                        selected_data.append({
                            "id": item["id"],
                            "title": item["title"],
                            "kind": item["kind"],
                            "document_number": item["employee"]["document_number"],
                            "rut": item["employee"]["rut"],
                            "first_name": item["employee"]["first_name"],
                            "last_name": item["employee"]["last_name"],
                            "segundo_apellido": item["employee"]["segundo_apellido"],
                            "start_date": item["employee"]["start_date"],
                            "operations": operation["title"],  # Agregar la operación completada
                        })            

            selected_data = [
                {
                    "id": item["id"],
                    "title": item["title"],
                    "kind": item["kind"],

                    "document_number": item["employee"]["document_number"],
                    "rut": item["employee"]["rut"],
                    "first_name": item["employee"]["first_name"],
                    "last_name": item["employee"]["last_name"],
                    "segundo_apellido": item["employee"]["segundo_apellido"],
                    "start_date": item["employee"]["start_date"],
                    "operations":item["operations"]["title"],
                }
                for item in data
            ]
            df = pd.DataFrame(selected_data)
            #print(df)
            #df.to_sql('mi_tabla', con=connectionPg, if_exists='append', index=False)

            df.to_sql('workflow_api', engine, if_exists='append', index=False)

            print('inserta el dataframe en la tabla')

    print("Transacción exitosa")


    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    connectionPg.close()
    

except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")

