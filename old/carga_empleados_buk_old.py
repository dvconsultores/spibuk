#import cx_Oracle
from sqlalchemy import create_engine
import requests
import psycopg2
from datetime import datetime

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
    fecha_ini = datetime.now()
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
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'empleados_buk'")

    # Verificar si se encontraron resultados
    if cursor.fetchone():
        # La tabla existe, puedes eliminar los registros
        cursor.execute("DELETE FROM empleados_buk")
        connectionPg.commit()
        print("elimima empleados_buk")


    ##*************************************** API BUK
    api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees"
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
            #data = response.json()["data"]
            #selected_data = [
            #    {
            #        "person_id": item["person_id"],
            #        "id": item["id"],
            #        "first_name": item["first_name"],
            #        "surname": item["surname"],
            #        "second_surname": item["second_surname"],
            #        "full_name": item["full_name"],
            #        "document_number": item["document_number"],
            #        "rut": item["rut"]
            #    }
            #    for item in data
            #]
        
            #df = pd.DataFrame(selected_data)

            data = response.json()
            df = pd.DataFrame(data["data"])
            df = df[["person_id", "id", "first_name", "surname", "second_surname", "full_name", "document_number", "rut"]]

            #print(df)
            df.to_sql('empleados_buk', engine, if_exists='append', index=False)
            print('inserta el dataframe en la empleados_buk, van '+str(page)+' de '+str(total_pages))
            fecha_fin = datetime.now()
    print("Transacción exitosa")
    print(fecha_ini)
    print(fecha_fin)

    # Calcula la diferencia entre las fechas
    diferencia = fecha_fin - fecha_ini

    # Obtiene el tiempo transcurrido en minutos
    minutos_transcurridos = diferencia.total_seconds() / 60

    # Imprime el resultado
    print("Tiempo transcurrido:", minutos_transcurridos, "minutos")


    connectionPg.close()
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")

