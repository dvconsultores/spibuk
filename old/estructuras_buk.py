
##*************************************** crear modelos en postgre a partir de las API buk
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import requests

# Carga las variables de entorno desde el archivo .env
load_dotenv()
import os

postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")





try:

    ##*************************************** API BUK
    api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees"
    headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        postgre_con = create_engine(f'postgresql+psycopg2://{postgre_user}:{postgre_pass}@{postgre_host}/{postgre_service}')

        # Crea una sesión SQLAlchemy
        Session = sessionmaker(bind=postgre_con)
        session = Session()


        print("Datos de la API obtenidos con éxito.",data)
    else:
        print("Error al realizar la solicitud GET a la API. :", response.status_code)


    sql_query = 'SELECT * FROM EO_PERSONA'
    df = pd.read_sql(sql_query, engine)
    df.to_sql('EO_PERSONA',if_exists="replace",con=postgre_con)

    postgre_con.dispose()    
    print('TERMINÓ')

except create_engine.Error as error:
    print("Error al conectar a Oracle SPI:", error)

