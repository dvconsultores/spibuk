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
from multiprocessing import Pool



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

def cargar_dataframe(df):
    df.to_sql('empleados_buk', engine, if_exists='append', index=False)

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
    cursor_family = connectionPg.cursor()

    # Consulta para verificar si la tabla existe
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'empleados_buk'")
    cursor_family.execute("SELECT * FROM information_schema.tables WHERE table_name = 'family_buk'")


    # Verificar si se encontraron resultados
    if cursor.fetchone():
        # La tabla existe, puedes eliminar los registros
        cursor.execute("DELETE FROM empleados_buk")
        connectionPg.commit()
        print("elimima empleados_buk")


    # Verificar si se encontraron resultados
    if cursor_family.fetchone():
        # La tabla existe, puedes eliminar los registros
        cursor_family.execute("DELETE FROM family_buk")
        connectionPg.commit()
        print("elimima family_buk")

    ##*************************************** API BUK


    # Lectura de la API
    api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees"
    headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
    responseEmpleado = requests.get(api_url, headers=headers)

    if responseEmpleado.status_code == 200:
        dataEmpleado = responseEmpleado.json()
        total_pages = dataEmpleado["pagination"]["total_pages"]
        dataframes = []

        for page in range(1, total_pages + 1):
            # Obtener la página actual
            url = f"{api_url}?page={page}"
            headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
            response = requests.get(url, headers=headers)
            data = response.json()["data"]
            df = pd.DataFrame(data)
            df = df[["person_id", "id", "first_name", "surname", "second_surname", "full_name", "document_number", "rut"]]
            dataframes.append(df)
            print('inserta el dataframe en la empleados_buk, van '+str(page)+' de '+str(total_pages))
            selected_data_family=[]
            for item in data:
                for operation in item["family_responsabilities"]:
                    if operation["responsability_details"]:
                        for family in operation["responsability_details"]:
                            selected_data_family.append({
                                "person_id": item["person_id"],
                                "id": item["id"],
                                "full_name": item["full_name"],
                                "document_number": item["document_number"],
                                "family_id": family["id"],
                                "family_rut": family["rut"],
                                "family_gender": family["gender"],
                                "family_first_name": family["first_name"],
                                "family_first_surname": family["first_surname"],
                                "family_second_surname": family["second_surname"],
                                "family_birthday": family["birthday"],
                                "family_relation": family["relation"],
                            })
                        #print(selected_data_family)
            print('creando familiares ini')
            df_family = pd.DataFrame(selected_data_family)
            df_family.to_sql('family_buk', engine, if_exists='append', index=False)
            print('creando familiares fin')
                
        # Cargar DataFrames en paralelo
        with Pool(processes=4) as pool:
            pool.map(cargar_dataframe, dataframes)
    fecha_fin = datetime.now()
    print("Transacción exitosa")
    print(fecha_ini)
    print(fecha_fin)
    diferencia = fecha_fin - fecha_ini
    minutos_transcurridos = diferencia.total_seconds() / 60
    print("Tiempo transcurrido:", minutos_transcurridos, "minutos")
    connectionPg.close()
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")

