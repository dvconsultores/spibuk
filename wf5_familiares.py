"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
    Este programa actualiza la informacion de familiares (ta_parientes) en SPI a apartir de la 
    tabla family_buk de PostgreSQL.

"""

import cx_Oracle
#import requests
import psycopg2
#from datetime import datetime
from dotenv import load_dotenv
import os

# Carga las variables de entorno desde el archivo .env
load_dotenv()

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

dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_service)

try:
    ##*************************************** ORACLE SPI
    connectionOra = cx_Oracle.connect(oracle_user, oracle_pass, dsn)
    print("Conexión exitosa a Oracle SPI")

    cursorOracle = connectionOra.cursor()
   #conecta con la table de control de ingreso de empleados
    connectionPg = psycopg2.connect(
        dbname=postgre_service,
        user=postgre_user,
        password=postgre_pass,
        host=postgre_host,
        port=postgre_port
    )
    print("Conexión exitosa a PostgreSQL")
    cursorApiFamily = connectionPg.cursor()
    try:
        # Iniciar la transacción
        connectionOra.begin()  
        ##########connectionPg.autocommit = False ####esto se coloca para probar. Pero es recomendable este en automatico para que registre el LOG
        #sql_query = "SELECT * FROM family_buk"
        documento='31.661.242' # para pruebas
        sql_query = f"SELECT * FROM family_buk where document_number= '{documento}'"
        cursorApiFamily.execute(sql_query)
        results = cursorApiFamily.fetchall()
        employee_id=''
        for row in results:
            person_id_buk=row[0]
            id_buk=row[1]
            full_name_buk=row[2]
            document_number_buk=row[3].replace('.', '')
            family_id_buk=row[4]
            family_rut_buk=row[5].replace('.', '')
            family_gender_buk='1' if row[6]=='male' else '2'
            family_first_name_buk=(row[7] or "").upper()
            family_first_surname_buk=(row[8] or "").upper()
            family_second_surname_buk=(row[9] or "").upper()
            family_birthday_buk=row[10]
            family_relation_buk=row[11]
            id_parentesco='ND'
            if(family_relation_buk)== 'child':
                id_parentesco='HIJO'
            if(family_relation_buk)== 'father':
                id_parentesco='PADRE'
            if(family_relation_buk)== 'mother':
                id_parentesco='MADRE'
            if(family_relation_buk)== 'husband':
                id_parentesco='CONYU'
            sql_query = f"SELECT ID FROM EO_PERSONA WHERE NUM_IDEN = {document_number_buk}"
            #print('sql_query',sql_query)
            cursorOracle.execute(sql_query)
            results_correlativo = cursorOracle.fetchone()
            Buk_ID=results_correlativo[0]

            sql_query = f"SELECT * FROM ta_parientes WHERE NUM_IDEN = '{family_rut_buk}' and id_persona={Buk_ID}"
            #print('sql_query',sql_query)
            cursorOracle.execute(sql_query)
            pariente_oracle = cursorOracle.fetchone()
            # Si el pariente existe, actualizarlo
            actividad=''
            if pariente_oracle:
                id_pariente=pariente_oracle[2]
                actividad='SE ACTUALIZAN LOA CAMPOS: )'
                FlagCambios=0
                print(f"Pariente con id {Buk_ID} existe. DATOS SPI= 1N:{pariente_oracle[3]} 2N:{pariente_oracle[4]} 1A:{pariente_oracle[5]} 2A:{pariente_oracle[6]} . DATOS BUK= 1N:{family_first_name_buk} 1A:{family_first_surname_buk} 2A:{family_second_surname_buk} ")
            # Si el pariente no existe, crearlo
            else:
                sql_query = f"SELECT max(id_pariente) FROM ta_parientes"
                #print('sql_query',sql_query)
                cursorOracle.execute(sql_query)
                Max_pariente = cursorOracle.fetchone()[0]+1

                BUK_id_persona=Buk_ID
                BUK_id_pariente	=Max_pariente
                BUK_id_parentesco=id_parentesco
                partes = family_first_name_buk.split(" ")
                if len(partes) >= 2:
                    BUK_nombre1 = partes[0]
                    BUK_nombre2 = partes[1]
                else:
                    BUK_nombre1=family_first_name_buk[:120]
                    BUK_nombre2=''
                BUK_apellido1=family_first_surname_buk
                BUK_apellido2=family_second_surname_buk
                BUK_id_tipo_iden =1
                BUK_nacional='Venezolana'
                BUK_num_iden=family_rut_buk
                BUK_pasaporte=''
                BUK_fecha_na=family_birthday_buk
                BUK_ciudad_na=''
                BUK_id_entfe_na=''
                BUK_id_pais_na=''
                BUK_sexo=family_gender_buk
                BUK_edo_civil='C' if id_parentesco=='CONYU' else 'S'
                BUK_usrcre='ETL'
                BUK_discapacitado =0
                #print('1')
                values_ta_parientes = {   
                    'BUK_id_persona' :BUK_id_persona,
                    'BUK_id_pariente' :BUK_id_pariente,
                    'BUK_id_parentesco' :BUK_id_parentesco,
                    'BUK_nombre1' :BUK_nombre1,
                    'BUK_nombre2' :BUK_nombre2,
                    'BUK_apellido1' :BUK_apellido1,
                    'BUK_apellido2' :BUK_apellido2,
                    'BUK_id_tipo_iden' :BUK_id_tipo_iden,
                    'BUK_nacional' :BUK_nacional,
                    'BUK_num_iden' :BUK_num_iden,
                    'BUK_pasaporte' :BUK_pasaporte,
                    'BUK_fecha_na' :BUK_fecha_na,
                    'BUK_ciudad_na' :BUK_ciudad_na,
                    'BUK_id_entfe_na' :BUK_id_entfe_na,
                    'BUK_id_pais_na' :BUK_id_pais_na,
                    'BUK_sexo' :BUK_sexo,
                    'BUK_edo_civil' :BUK_edo_civil,
                    'BUK_usrcre' :BUK_usrcre,
                    'BUK_discapacitado':BUK_discapacitado,
                } 	
                sql_query = "INSERT INTO ta_parientes (id_persona,id_pariente,id_parentesco,nombre1,nombre2,apellido1,apellido2,"+ \
                 "id_tipo_iden,nacional,num_iden,pasaporte,fecha_na,ciudad_na,id_entfe_na,"+ \
                 "id_pais_na,sexo,edo_civil,usrcre,feccre,discapacitado)"+ \
                 "  VALUES (:BUK_id_persona,:BUK_id_pariente,:BUK_id_parentesco,:BUK_nombre1,:BUK_nombre2,:BUK_apellido1,:BUK_apellido2,"+ \
                 ":BUK_id_tipo_iden,:BUK_nacional,:BUK_num_iden,:BUK_pasaporte,TO_DATE(:BUK_fecha_na, 'YYYY-MM-DD'),:BUK_ciudad_na,:BUK_id_entfe_na,"+ \
                 ":BUK_id_pais_na,:BUK_sexo,:BUK_edo_civil,:BUK_usrcre,SYSDATE,:BUK_discapacitado)"
                #print('sql_query',sql_query)
                cursorOracle.execute(sql_query,values_ta_parientes)

                connectionOra.commit()
                print(f"Pariente con id {Buk_ID} creado. id_parente {BUK_id_pariente}")
            #endif

        #ENDFOR
        # Confirmar la transacción si no hubo errores
        connectionOra.commit()
        #connectionOra.rollback()
        # LISTA EL CONTENIDO DEL LOG
        #sql_query = "SELECT * FROM log"
        #cursorApiFamily.execute(sql_query)
        #results_log = cursorApiFamily.fetchall()
        #for row in results_log:
        #    print(row)
        connectionPg.commit()
        #connectionPg.rollback()
        print("Transacción finaizada")
    except cx_Oracle.DatabaseError as e:
        # Manejar excepciones relacionadas con la base de datos
        print("Error de base de datos:", e)
        # Realizar rollback en caso de error
        #connectionOra.rollback()
        print("Rollback realizado")
    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    cursorApiFamily.close()
    connectionPg.close()
    connectionOra.close()
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
