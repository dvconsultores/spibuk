"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
 Este programa carga los empleados, familiares de la API de BUK en la tabla empleados_buk, family_buk de PostgreSQL 
 respectivamente.

"""

#import cx_Oracle
from sqlalchemy import create_engine
import requests
import psycopg2
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
from multiprocessing import Pool

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# Credenciales PostgreSQL desde variables de entorno
postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")

def cargar_dataframe(df):
    # Crear una nueva conexión para cada proceso
    engine = create_engine(f'postgresql://{postgre_user}:{postgre_pass}@{postgre_host}:{postgre_port}/{postgre_service}')
    with engine.begin() as connection:
        df.to_sql('empleados_buk', connection, if_exists='append', index=False)
    engine.dispose()  # Cerrar la conexión después de usar

try:
    #fecha_ini = datetime.now()
    
    # Conexión a PostgreSQL usando psycopg2 (solo para operaciones iniciales)
    connectionPg = psycopg2.connect(
        dbname=postgre_service,
        user=postgre_user,
        password=postgre_pass,
        host=postgre_host,
        port=postgre_port
    )
    print("Conexión exitosa a PostgreSQL")
    
    # Crear un cursor
    cursor = connectionPg.cursor()
    cursor_family = connectionPg.cursor()

    # Verificar y eliminar registros existentes
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'empleados_buk'")
    if cursor.fetchone():
        cursor.execute("DELETE FROM empleados_buk")
        connectionPg.commit()
        #print("Registros de empleados_buk eliminados")

    cursor_family.execute("SELECT * FROM information_schema.tables WHERE table_name = 'family_buk'")
    if cursor_family.fetchone():
        cursor_family.execute("DELETE FROM family_buk")
        connectionPg.commit()
        #print("Registros de family_buk eliminados")

    # Configurar el motor de SQLAlchemy (usado en el hilo principal)
    engine_main = create_engine(f'postgresql://{postgre_user}:{postgre_pass}@{postgre_host}:{postgre_port}/{postgre_service}')

    # Lectura de la API BUK
    api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees"
    headers = {'auth_token': os.getenv('AUTH_TOKEN')}
    responseEmpleado = requests.get(api_url, headers=headers)

    if responseEmpleado.status_code == 200:
        dataEmpleado = responseEmpleado.json()
        total_pages = dataEmpleado["pagination"]["total_pages"]
        dataframes = []

        for page in range(1, total_pages + 1):
            url = f"{api_url}?page={page}"
            response = requests.get(url, headers=headers)
            data = response.json()["data"]
            df = pd.DataFrame(data)
            df = df[["person_id", "id", "first_name", "surname", "second_surname", "full_name", "document_number", "rut"]]
            dataframes.append(df)
            print(f'Procesando página {page} de {total_pages}')

            # Procesar familiares
            selected_data_family = []
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
            
            # Insertar familiares en el hilo principal
            if selected_data_family:
                df_family = pd.DataFrame(selected_data_family)
                with engine_main.begin() as conn:
                    df_family.to_sql('family_buk', conn, if_exists='append', index=False)
                #print('Familiares insertados correctamente')

        # Procesar empleados en paralelo
        with Pool(processes=4) as pool:
            pool.map(cargar_dataframe, dataframes)

    #echa_fin = datetime.now()
    #print("Transacción finalizada")
    #print(f"Inicio: {fecha_ini}")
    #print(f"Fin: {fecha_fin}")
    #print(f"Tiempo transcurrido: {(fecha_fin - fecha_ini).total_seconds() / 60} minutos")
    #connectionPg.close()

except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
    connectionPg.rollback()
except Exception as e:
    print(f"Error general: {e}")
    if 'connectionPg' in locals():
        connectionPg.rollback()