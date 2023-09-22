
##*************************************** crear modelos en postgre a aprtir de las talas en oracle Infocen
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()
import os

oracle_user = os.getenv("ORACLE_USER")
oracle_pass = os.getenv("ORACLE_PASS")
oracle_host = os.getenv("ORACLE_HOST")
oracle_port = os.getenv("ORACLE_PORT")
oracle_service = os.getenv("ORACLE_SERVICE")

postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")


engine = create_engine(f'oracle://{oracle_user}:{oracle_pass}@{oracle_host}:{oracle_port}/{oracle_service}')
postgre_con = create_engine(f'postgresql+psycopg2://{postgre_user}:{postgre_pass}@{postgre_host}/{postgre_service}')


try:
    ##*************************************** ORACLE SPI


    sql_query = 'SELECT * FROM EO_PERSONA'
    df = pd.read_sql(sql_query, engine)
    df.to_sql('EO_PERSONA',if_exists="replace",con=postgre_con)

    sql_query1 = 'SELECT * FROM TA_RELACION_LABORAL'
    df = pd.read_sql(sql_query1, engine)
    df.to_sql('TA_RELACION_LABORAL',if_exists="replace",con=postgre_con)

    sql_query2 = 'SELECT * FROM TA_RELACION_PUESTO'
    df = pd.read_sql(sql_query2, engine)
    df.to_sql('TA_RELACION_PUESTO',if_exists="replace",con=postgre_con)

    sql_query3 = 'SELECT * FROM TA_PARIENTES'
    df = pd.read_sql(sql_query3, engine)
    df.to_sql('TA_PARIENTES',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM EO_CARGO'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('EO_CARGO',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM EO_EMPRESA'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('EO_EMPRESA',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM EO_ESTADO_CIVIL'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('EO_ESTADO_CIVIL',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM EO_PUESTO'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('EO_PUESTO',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM EO_TIPO_IDENTIFICACION'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('EO_TIPO_IDENTIFICACION',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM EO_UNIDAD'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('EO_UNIDAD',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM SPI_ENTIDAD_FEDERAL'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('SPI_ENTIDAD_FEDERAL',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM SPI_PAISES'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('SPI_PAISES',if_exists="replace",con=postgre_con)

    sql_query4 = 'SELECT * FROM TA_PARENTESCOS'
    df = pd.read_sql(sql_query4, engine)
    df.to_sql('TA_PARENTESCOS',if_exists="replace",con=postgre_con)


    engine.dispose()
    postgre_con.dispose()    
    print('TERMINÓ')

except create_engine.Error as error:
    print("Error al conectar a Oracle SPI:", error)

