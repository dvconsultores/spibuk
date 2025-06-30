"""
Empresa: DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Junio 2025
Descripción:
 Este programa carga los periodos desde la API de BUK a la tabla Periodos en PostgreSQL.
 Se borran todos los registros existentes antes de insertar los nuevos.
"""
from sqlalchemy import create_engine
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
import subprocess
import os

# Obtener la ruta del directorio actual
directorio_actual = os.path.dirname(os.path.abspath(__file__))
# Carga las variables de entorno desde el archivo .env
load_dotenv()

postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")

programas = [
    "api0_actualiza.py",
    "api0_promocion.py",
    "wf1_carga_empleados_buk.py",
    "wf2_carga_workflow.py",
    "wf3_envio_correo.py",
    "wf4_ingreso.py",
]


try:
    # Conexión a PostgreSQL
    connectionPg = psycopg2.connect(
        dbname=postgre_service,
        user=postgre_user,
        password=postgre_pass,
        host=postgre_host,
        port=postgre_port
    )
    print("Conexión exitosa a PostgreSQL")
    engine = create_engine(f'postgresql://{postgre_user}:{postgre_pass}@{postgre_host}:{postgre_port}/{postgre_service}')
    
    from datetime import datetime
    
    # Verificar si la tabla Periodos existe
    cursor = connectionPg.cursor()
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'periodos'")
    
    if cursor.fetchone():
        # Validar periodos abiertos
        fecha_actual = datetime.now().strftime("%Y-%m-%d")
        
        # Contar todos los periodos abiertos
        cursor.execute("SELECT COUNT(*) FROM periodos WHERE status = 'abierto'")
        total_abiertos = cursor.fetchone()[0]
        print(total_abiertos, "periodos abiertos encontrados")
        # Contar periodos abiertos válidos (month >= fecha actual)
        cursor.execute("""
            SELECT COUNT(*)
            FROM periodos
            WHERE status = 'abierto'
            AND end_date >= %s
        """, (fecha_actual,))
        total_validos = cursor.fetchone()[0]
        print(total_validos, "periodos abiertos válidos encontrados")
        
        if total_abiertos != total_validos:
            print("HAY PERIODOS ABIERTOS FUERA DE RANGO")
            connectionPg.close()
            exit()
        else:


            fecha_ini = datetime.now()
            errores = []

            for programa in programas:
                # Construir la ruta completa al archivo 
                ruta_completa = os.path.join(directorio_actual, programa)

                print(f"Ejecutando {ruta_completa}...")

                try:
                    # Ejecutar el script usando el intérprete Python
                    subprocess.run(["python3", ruta_completa], check=True)
                    print(f"{ruta_completa} se ejecutó correctamente.")
                except subprocess.CalledProcessError as e:
                    errores.append(f"Error al ejecutar {ruta_completa}: {e}")
                    print(f"Error al ejecutar {ruta_completa}: {e}")
                except Exception as e:
                    errores.append(f"Error inesperado al ejecutar {ruta_completa}: {e}")
                    print(f"Error inesperado al ejecutar {ruta_completa}: {e}")

            if errores:
                print("Se encontraron errores durante la ejecución de los programas:")
                for error in errores:
                    print(error)
            else:
                print("Todos los programas se han ejecutado correctamente.")

            fecha_fin = datetime.now()
            print(f"Inicio: {fecha_ini}")
            print(f"Fin: {fecha_fin}")
            print(f"Tiempo transcurrido: {(fecha_fin - fecha_ini).total_seconds() / 60} minutos")





        # Borrar registros existentes si pasa validación
        cursor.execute("DELETE FROM periodos")
        connectionPg.commit()
        print("Registros existentes en tabla periodos borrados")

    # Consulta a la API de BUK con paginación
    api_url = "https://alfonzorivas.buk.co/api/v1/chile/process_periods"
    headers = {'auth_token': os.getenv('AUTH_TOKEN')}
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        total_pages = data["pagination"]["total_pages"]
        periodos_data = []
        
        for page in range(1, total_pages + 1):
            print(f'Procesando página {page} de {total_pages}')
            url = f"{api_url}?page={page}"
            response = requests.get(url, headers=headers)
            data = response.json()["data"]
            
            # Procesar cada periodo
            for periodo in data:
                periodos_data.append({
                    "id": periodo["id"],
                    "month": periodo["month"],
                    "end_date": periodo["end_date"],
                    "status": periodo["status"]
                })
        
        # Crear DataFrame e insertar en PostgreSQL
        df = pd.DataFrame(periodos_data)
        df.to_sql('periodos', engine, if_exists='append', index=False)
        print(f"{len(periodos_data)} periodos insertados correctamente")

    # Cerrar conexión
    connectionPg.commit()
    connectionPg.close()
    print("Transacción finalizada")

except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
except Exception as e:
    print(f"Error general: {e}")