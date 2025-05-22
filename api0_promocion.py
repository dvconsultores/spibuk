"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
 Este programa gestiona las promociones de los empleados las transferencias y las reclasificaciones
 a partir de lA tabla empleados de PostgreSQL.

"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import cx_Oracle
import requests
import psycopg2
from datetime import datetime


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

print('postgre_service',postgre_pass)

email_user = os.getenv("EMAIL_USER")
email_pass = os.getenv("EMAIL_PASS")



dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_service)



try:
    #  Configura la conexión al servidor SMTP
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(email_user, email_pass)
    print("Conexión exitosa a smtp")

    ##*************************************** ORACLE SPI
    connection = cx_Oracle.connect(oracle_user, oracle_pass, dsn)
    print("Conexión exitosa a Oracle SPI")

    cursor = connection.cursor()
   #conecta con la table de control de ingreso de empleados
    connectionPg = psycopg2.connect(
        dbname=postgre_service,
        user=postgre_user,
        password=postgre_pass,
        host=postgre_host,
        port=postgre_port
    )
    print("Conexión exitosa a PostgreSQL")
    cursorApiEmpleado = connectionPg.cursor()
    try:
        # Iniciar la transacción
        connection.begin()
        ##########connectionPg.autocommit = False ####esto se coloca para probar. Pero es recomendable este en automatico para que registre el LOG
        #sql_query = "SELECT * FROM empleados where ID  in (29230) and status_process is null"
        sql_query = "SELECT * FROM empleados where event_type  in ('employee_update','job_movement') and status_process is null"
        cursorApiEmpleado.execute(sql_query)
        results = cursorApiEmpleado.fetchall()
        employee_id=''
        for row in results:
            employee_id=row[0]
            date_id=row[1]
            event_type_id=row[2]
            transacction_id=row[6]
            name_id=row[7]
            ci_id=row[8]
            company_id=row[9]
            #print('empleado json',employee_id,'',transacction_id,ci_id,company_id)

            ##*************************************** API BUK
            api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees/"+employee_id
            headers = {'auth_token': os.getenv('AUTH_TOKEN')}
            responseEmpleado = requests.get(api_url, headers=headers)
            if responseEmpleado.status_code == 200:
                dataEmpleado = responseEmpleado.json()
                Buk_COMPANY=dataEmpleado.get("data", []).get("current_job", {}).get("company_id")  #CODIGO COMPAÑIA BUK
                print("Datos de la API obtenidos con éxito.",'dataEmpleado',Buk_COMPANY)
            else:
                print("Error al realizar la solicitud GET a la API. :", responseEmpleado.status_code)
            Buk_FICHA_JSON=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Ficha"))[:30]
            Buk_NUM_IDEN=(dataEmpleado.get("data", []).get("rut"))[:20]
            #print('Buk_NUM_IDEN',Buk_NUM_IDEN.replace('.', ''))
            #print('Buk_FICHA_JSON',Buk_FICHA_JSON)
            sql_query = 'SELECT COUNT(*) FROM EO_PERSONA WHERE NUM_IDEN = '+Buk_NUM_IDEN.replace('.', '')
            cursor.execute(sql_query)
            count_result = cursor.fetchone()[0]
            # Verificar si el COLABORADOR EXISTE
            if count_result == 0:
                # L   O   G   ****************************************************************
                Actividad = "El colaborador RUT="+Buk_NUM_IDEN.replace('.', '')+" NO EXISTE."
                print(Actividad)
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
            else:
                print(f"El colaborador EXISTE: {count_result}")
                sql_query = 'SELECT * FROM EO_PERSONA WHERE NUM_IDEN = '+Buk_NUM_IDEN.replace('.', '')
                #print('sql_query',sql_query)
                cursor.execute(sql_query)
                results_eo_persona = cursor.fetchone()
                Buk_ID=results_eo_persona[0]
                # tomamos un nuevo numero de ficha paa el caso de las transferencias
                sql_query = "SELECT * FROM correlativos"
                cursorApiEmpleado.execute(sql_query)
                results_correlativo = cursorApiEmpleado.fetchone()
                Buk_FICHA_NEW=results_correlativo[0]  #SOLO PARA TRANSFERENCIAS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                
                #print('id empleado',Buk_ID)
                # L   O   G   ****************************************************************
                Actividad = "El colaborador RUT="+Buk_NUM_IDEN.replace('.', '')+" EXISTE."
                print(Actividad)
                Estatus = "INFO"
                fecha_actual = datetime.now()
                # consulta = "INSERT INTO public.log "+ \
                # "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                # "VALUES(%s, %s, %s, %s, %s, %s)"
                # cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
            api_url = "https://alfonzorivas.buk.co/api/v1/colombia/companies"
            #headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
            headers = {'auth_token': os.getenv('AUTH_TOKEN')}
            responseEmpresa = requests.get(api_url, headers=headers)
            if responseEmpresa.status_code == 200:
                dataEmpresa = responseEmpresa.json() 
                #print("Datos de la API/companies obtenidos con éxito.",dataEmpresa)
                result = [item for item in dataEmpresa.get("data", []) if item.get("id") == Buk_COMPANY]
                if result:
                    elemento_encontrado = result[0]
                    ID_EMPRESA = elemento_encontrado.get("custom_attributes", {}).get("codigo_empresa") #CODIGO COMPAÑIA BUK-> SPI
                    #print(f"El valor de 'codigo_empresa' es: {ID_EMPRESA,}")
                else:
                    print("No se encontró ningún elemento con ID igual a 10.")

            else:
                print("Error al realizar la solicitud GET a la API. :", responseEmpresa.status_code)


            #determina el id del colaborador
            Buk_FICHA = (dataEmpleado.get("data", [])
                                .get("custom_attributes", {})
                                .get("Ficha", "")
                                .strip()
                                .upper())
            #print('ficha segun buk',Buk_FICHA)
            #determina el id de la ENTIDAD FEDERAL
            Buk_ESTADO = (dataEmpleado.get("data", [])
                                .get("custom_attributes", {})
                                .get("Estado", "")
                                .strip()
                                .upper())
            

            parametros = {
                'Buk_ESTADO':Buk_ESTADO,
            }
            consulta = """
                    SELECT CODIGO FROM SPI_ENTIDAD_FEDERAL WHERE CODIGO_PAIS = 'VEN' AND UPPER(TRIM(NOMBRE)) = :Buk_ESTADO
            """
            #print(consulta)
            cursor.execute(consulta, parametros)
            Buk_ID_ENTFE_NA = cursor.fetchone()[0]
            #print('Buk_ESTADO:',Buk_ESTADO,'Buk_ID_ENTFE_NA:',Buk_ID_ENTFE_NA)

            #DETERMINAR EL CODIGO DE LOCALIDAD
            Buk_SEDE=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Sede").upper())[:30]
            Actividad = "Se crea una relación laboral en TA_RELACION_LABORAL."
            Estatus = "INFO"
            fecha_actual = datetime.now()
            consulta = "Select  codloc from public.localidades where upper(Sede)=%s and CIA_CODCIA=%s  "
            cursorApiEmpleado.execute(consulta, (Buk_SEDE, ID_EMPRESA))
            print(Buk_SEDE, ID_EMPRESA)
            results = cursorApiEmpleado.fetchone()
            if results is None:
                Buk_SEDE_Estado=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Estado").upper())[:30]
                Buk_SEDE = f"%{Buk_SEDE}%"
                consulta = "SELECT codloc FROM public.localidades WHERE UPPER(Sede) LIKE %s AND CIA_CODCIA = %s AND UPPER(Estado) = %s"
                cursorApiEmpleado.execute(consulta, (Buk_SEDE, ID_EMPRESA, Buk_SEDE_Estado))
                print(Buk_SEDE, ID_EMPRESA)
                results = cursorApiEmpleado.fetchone()
                if results is None:
                    # L   O   G   ****************************************************************
                    Actividad = 'Buk reporta la localidad: '+str(Buk_SEDE)+ 'y no existe en loclidades.'
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    Buk_LOCALIDAD='999' # SIN LOCALIDAD
                    #####*********************************************
                else:
                    # L   O   G   ****************************************************************
                    Actividad = 'Buk reporta la localidad: '+str(Buk_SEDE)+ ' y no existe en loclidades. Pero se encuentra la localidad por estado.'
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    Buk_LOCALIDAD='999' # SIN LOCALIDAD
                    #####*********************************************                    
                    Buk_LOCALIDAD=results[0]
            else:
                Buk_LOCALIDAD=results[0]
            print('Buk_LOCALIDAD',Buk_LOCALIDAD)
            Buk_NOMBRE=(dataEmpleado.get("data", []).get("first_name"))[:120]
            partes = Buk_NOMBRE.split(" ")
            if len(partes) >= 2:
                Buk_NOMBRE1 = partes[0].upper()
                Buk_NOMBRE2 = partes[1].upper()
            else:
                Buk_NOMBRE1=(dataEmpleado.get("data", []).get("first_name")).upper()[:120]
                Buk_NOMBRE2=""
            Buk_APELLIDO1=(dataEmpleado.get("data", []).get("surname")).upper()[:17]
            Buk_APELLIDO2 = (dataEmpleado.get("data", {}).get("second_surname") or "").upper()[:15]
            Buk_ID_TIPO_IDEN='1'
            Buk_NACIONAL=(dataEmpleado.get("data", []).get("nationality"))[:50]
            Buk_NUM_IDEN=(dataEmpleado.get("data", []).get("rut")).replace('.', '')[:20]
           #print('Buk_NUM_IDEN',Buk_NUM_IDEN)
            Buk_PASAPORTE=(dataEmpleado.get("data", []).get("rut")).replace('.', '')[:10]
            Buk_FECHA_NA=(dataEmpleado.get("data", []).get("birthday"))
            Buk_CIUDAD_NA=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Ciudad"))[:30]
            if (dataEmpleado.get("data", []).get("country_code"))=='VE':
                Buk_ID_PAIS_NA='VEN'
            Buk_SEXO='1' if dataEmpleado.get("data", []).get("gender")=='M' else '2'          
            Buk_EDO_CIVIL = (dataEmpleado.get("data", {}).get("civil_status", "")[:1] or "").upper()
            if Buk_EDO_CIVIL =="U":
                Buk_EDO_CIVIL="L"
            #print('Buk_EDO_CIVIL',Buk_EDO_CIVIL)
            Buk_ZURDO=0
            Buk_TIPO_SANGRE=' '
            Buk_FACTOR_RH=' '
            Buk_DIRECCION=(dataEmpleado.get("data", []).get("address"))[:120]
            Buk_CIUDAD=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Ciudad"))[:30]
            Buk_ID_ENTFE=Buk_ID_ENTFE_NA
            Buk_ID_PAIS=Buk_ID_PAIS_NA
            Buk_PARROQUIA=' '
            Buk_MUNICIPIO=' '
            Buk_COD_POSTAL=' '
            Buk_TELEFONO1=(dataEmpleado.get("data", []).get("office_phone"))
            Buk_TELEFONO2=' '
            Buk_FAX=' '
            try:
                Buk_CELULAR = dataEmpleado.get("data", []).get("phone")[:120]
            except TypeError:
                Buk_CELULAR = ""  


            # esta es a logica de ingreso de colaboradores
            if dataEmpleado.get("data", []).get("email"):
                Buk_E_MAIL1=(dataEmpleado.get("data", []).get("email"))[:120]
            else:
                Buk_E_MAIL1=''

            if dataEmpleado.get("data", []).get("personal_email"):
                Buk_E_MAIL2=(dataEmpleado.get("data", []).get("personal_email"))[:120]
            else:
                Buk_E_MAIL2=''

            # esta en la logica anterior
            #if dataEmpleado.get("data", []).get("personal_email"):
            #    Buk_E_MAIL1=(dataEmpleado.get("data", []).get("personal_email"))[:120]
            #else:
            #    Buk_E_MAIL1=''
            #if dataEmpleado.get("data", []).get("email"):
            #    Buk_E_MAIL2=(dataEmpleado.get("data", []).get("email"))[:120]
            #else:
            #    Buk_E_MAIL2=''

            Buk_IN_REL_TRAB=''
            Buk_USRCRE='ETL'
            Buk_USRACT='ETL'
            Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))

            if count_result == 1:         
                if results_eo_persona[1] != Buk_NOMBRE1:
                     # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en NOMBRE1= SPI:'+str(results_eo_persona[1])+" BUK:"+str(Buk_NOMBRE1)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[2] != Buk_NOMBRE2:
                    # L   O   G   ****************************************************************
                    Actividad = f'Diferencia en NOMBRE2= SPI: {results_eo_persona[2] or "N/A"} BUK: {Buk_NOMBRE2 or ""}'
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[3] != Buk_APELLIDO1:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en APELLIDO1= SPI:'+str(results_eo_persona[3])+" BUK:"+str(Buk_APELLIDO1)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[4] != Buk_APELLIDO2:
                    # L   O   G   ****************************************************************
                    Actividad = f'Diferencia en APELLIDO2= SPI: {results_eo_persona[4] or "N/A"} BUK: {Buk_APELLIDO2 or ""}'
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[5] != Buk_ID_TIPO_IDEN:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'ID_TIPO_IDEN':str(Buk_ID_TIPO_IDEN),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET ID_TIPO_IDEN=:ID_TIPO_IDEN
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en ID_TIPO_IDEN= SPI:'+str(results_eo_persona[5])+" BUK:"+str(Buk_ID_TIPO_IDEN)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[6] != Buk_NACIONAL:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'NACIONAL':str(Buk_NACIONAL),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET NACIONAL=:NACIONAL
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en NACIONAL= SPI:'+str(results_eo_persona[6])+" BUK:"+str(Buk_NACIONAL)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[7] != Buk_NUM_IDEN:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en NUM_IDEN= SPI:'+str(results_eo_persona[7])+" BUK:"+str(Buk_NUM_IDEN)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[8] != Buk_PASAPORTE:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en PASAPORTE= SPI:'+results_eo_persona[8]+" BUK:"+Buk_PASAPORTE
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[9].strftime('%Y-%m-%d') != Buk_FECHA_NA:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en FECHA_NA= SPI:'+str(results_eo_persona[9].strftime('%Y-%m-%d'))+" BUK:"+str(Buk_FECHA_NA)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[10] != Buk_CIUDAD_NA:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'CIUDAD_NA':str(Buk_CIUDAD_NA),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET CIUDAD_NA=:CIUDAD_NA
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en CIUDAD_NA= SPI:'+str(results_eo_persona[10])+" BUK:"+str(Buk_CIUDAD_NA)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[11] != Buk_ID_ENTFE_NA:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'ID_ENTFE_NA':str(Buk_ID_ENTFE_NA),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET ID_ENTFE_NA=:ID_ENTFE_NA
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en ID_ENTFE_NA= SPI:'+str(results_eo_persona[11])+" BUK:"+str(Buk_ID_ENTFE_NA)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[12] != Buk_ID_PAIS_NA:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'ID_PAIS_NA':str(Buk_ID_PAIS_NA),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET ID_PAIS_NA=:ID_PAIS_NA
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en ID_PAIS_NA= SPI:'+str(results_eo_persona[12])+" BUK:"+str(Buk_ID_PAIS_NA)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[13] != Buk_SEXO:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'SEXO':(Buk_SEXO),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET SEXO=:SEXO
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en SEXO= SPI:'+str(results_eo_persona[13])+" BUK:"+str(Buk_SEXO)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[14] != Buk_EDO_CIVIL:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'EDO_CIVIL':str(Buk_EDO_CIVIL),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET EDO_CIVIL=:EDO_CIVIL
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en EDO_CIVIL= SPI:'+str(results_eo_persona[14])+" BUK:"+str(Buk_EDO_CIVIL)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[15] != Buk_ZURDO:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en ZURDO= SPI:'+str(results_eo_persona[15])+" BUK:"+str(Buk_ZURDO)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[18] != Buk_DIRECCION:
                    print('Diferencia en DIRECCION= SPI:',results_eo_persona[18]," BUK:",Buk_DIRECCION)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en DIRECCION= SPI:'+str(results_eo_persona[18])+" BUK:"+str(Buk_DIRECCION)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[19] != Buk_CIUDAD:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'CIUDAD':str(Buk_CIUDAD),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET CIUDAD=:CIUDAD
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    print('Diferencia en CIUDAD= SPI:',results_eo_persona[19]," BUK:",Buk_CIUDAD)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en CIUDAD= SPI:'+str(results_eo_persona[19])+" BUK:"+str(Buk_CIUDAD)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[20] != Buk_ID_ENTFE:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'ID_ENTFE':str(Buk_ID_ENTFE),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET ID_ENTFE=:ID_ENTFE
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en ID_ENTFE= SPI:'+str(results_eo_persona[20])+" BUK:"+str(Buk_ID_ENTFE)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[21] != Buk_ID_PAIS:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'ID_PAIS':str(Buk_ID_PAIS),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET ID_PAIS=:ID_PAIS
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en ID_PAIS= SPI:'+str(results_eo_persona[21])+" BUK:"+str(Buk_ID_PAIS)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[25] != Buk_TELEFONO1:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en TELEFONO1= SPI:'+str(results_eo_persona[25])+" BUK:"+str(Buk_TELEFONO1)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[28] != Buk_CELULAR:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en CELULAR= SPI:'+str(results_eo_persona[28])+" BUK:"+str(Buk_CELULAR)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[29] != Buk_E_MAIL1:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'E_MAIL1':str(Buk_E_MAIL1),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET E_MAIL1=:E_MAIL1
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en E_MAIL1= SPI:'+str(results_eo_persona[29])+" BUK:"+str(Buk_E_MAIL1)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                if results_eo_persona[30] != Buk_E_MAIL2:
                    parametros = {
                        'NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                        'E_MAIL2':str(Buk_E_MAIL2),
                    }
                    consulta = """
                        UPDATE EO_PERSONA SET E_MAIL2=:E_MAIL2
                            WHERE NUM_IDEN = :NUM_IDEN
                        """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en E_MAIL2= SPI:'+str(results_eo_persona[30])+" BUK:"+str(Buk_E_MAIL2)+". Update en EO_PERSONA"
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************

                if results_eo_persona[31] != Buk_IN_REL_TRAB:
                    # L   O   G   ****************************************************************
                    Actividad = 'Diferencia en IN_REL_TRAB= SPI:'+str(results_eo_persona[31])+" BUK:"+str(Buk_IN_REL_TRAB)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
       
                #BUSCAR INFORMACION EN TA_RELACION_LABORAL PARA VALIDAR SI HAY CAMBIOS EN LA FICHA DEL JEFE O EN LA LOCALIDAD O SI LA EMPRESA ES L AMISMA QUE EN BUK (CASO TRANSFERENCIA) 
                sql_query = "SELECT ID_EMPRESA empresa,FICHA,FICHA_JEFE,ID_LOCALIDAD " + \
                "FROM TA_RELACION_LABORAL trl ,EO_PERSONA ep "+ \
                "WHERE trl.ID_PERSONA  = ep.ID " + \
                "AND ID_EMPRESA!='BA' AND F_RETIRO IS NULL AND ep.NUM_IDEN ="+Buk_NUM_IDEN.replace('.', '')
                cursor.execute(sql_query)
                results_transferencia = cursor.fetchone()
                ID_EMPRESA_RelacionLaboral=results_transferencia[0]
                FICHA_RelacionLaboral=results_transferencia[1]
                FICHA_JEFE_RelacionLaboral=results_transferencia[2] if results_transferencia[2] != None else 'Sin Coach'
                ID_LOCALIDAD_RelacionLaboral=results_transferencia[3]


                #VARIABLES A UTILIZAR EN AS DIFERENTES VALIDACIONES
                id_cambio=''
                #Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))
                Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
                if isinstance((dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo")), str):
                    Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))[:5]
                else:
                    Buk_ID_CARGO=int(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))
                    Buk_ID_CARGO=str(Buk_ID_CARGO)
                Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado")).upper()
                Buk_Nivel_de_Seniority=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Nivel_de_Seniority")).upper()
                Buk_ID_CARGO_NOMBRE_SR= Buk_ID_CARGO_NOMBRE+" "+Buk_Nivel_de_Seniority
                #print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
                #print('Buk_ID_CARGO',Buk_ID_CARGO)
                #print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO_NOMBRE)
                #print('Buk_Nivel_de_Seniority',Buk_Nivel_de_Seniority)
                #print('Buk_ID_CARGO_NOMBRE_SR',Buk_ID_CARGO_NOMBRE_SR)

                #REVISAR SI EXISTE EL CENTRO DE COSTO   // jorge cuauro 04-2025 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                parametros = {
                        'id_empresa':company_id,
                        'Buk_ID_UNIDAD':Buk_ID_UNIDAD,
                    }
                consulta = """
                    SELECT COUNT(*) AS CUENTA  
                    FROM EO_UNIDAD eu
                    WHERE eu.ID_EMPRESA = :id_empresa AND eu.ID = :Buk_ID_UNIDAD
                """
                #print(consulta)
                cursor.execute(consulta, parametros)

                count_EO_UNIDAD = cursor.fetchone()[0]
                if count_EO_UNIDAD==0:
                    #print('11')
                    Actividad = 'El centro de costo='+str(Buk_ID_UNIDAD)+' para la empresa='+str(company_id)+' NO existe.'
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************  
                    if isinstance((dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("area_ids")), str):
                        Buk_ID_AREA=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("area_ids"))[:5]
                    else:
                        Buk_ID_AREA=dataEmpleado.get("data", {}).get("current_job", {}).get("role", {}).get("area_ids", [])
                    #print('Buk_ID_AREA',Buk_ID_AREA)
                    Buk_name_Area=''
                    for area_id in Buk_ID_AREA:

                        api_url = "https://alfonzorivas.buk.co/api/v1/colombia/organization/areas/"+str(area_id)
                        headers = {'auth_token': os.getenv('AUTH_TOKEN')}
                        responseArea = requests.get(api_url, headers=headers)
                        if responseArea.status_code == 200:
                            dataArea = responseArea.json()
                            Buk_cost_center_Area=dataArea.get("data", []).get("cost_center")  #Nombre centro de costo
                            if Buk_ID_UNIDAD==Buk_cost_center_Area: #deber el nombre donde el centro de costo sea elmismo de la api de empleados
                                Buk_name_Area=dataArea.get("data", []).get("name")
                            #print("Datos de la API obtenidos con éxito.",'dataArea',Buk_name_Area,Buk_cost_center_Area)
                        else:
                            print("Error al realizar la solicitud GET a la API. :", responseArea.status_code)

                    if Buk_name_Area:
                        # CREAR LA NUEVA EO_UNIDAD+++++++++++++++++++++++++++++++++++++++++++++++++
                        values_eo_cargo = {   
                            'Buk_ID_EMPRESA' :company_id,
                            'Buk_name_Area' :Buk_name_Area,
                            'Buk_ID_UNIDAD':Buk_ID_UNIDAD,
                            'Buk_USRCRE' :Buk_USRCRE,
                        }  
                        sql_query = "INSERT INTO INFOCENT.EO_UNIDAD "+ \
                        "(      ID_EMPRESA, ID,  NOMBRE,  USRCRE,  FECCRE, FECHA_INI) "+ \
                        "VALUES(:Buk_ID_EMPRESA,  :Buk_ID_UNIDAD ,:Buk_name_Area, :Buk_USRCRE, SYSDATE, TO_DATE('01-01-1900', 'DD-MM-YYYY'))"
                        cursor.execute(sql_query,values_eo_cargo)
                        # L   O   G   ****************************************************************
                        Actividad = "Se crea en EO_UNIDAD el Unidad Código="+str(Buk_ID_UNIDAD)+' Unidad='+Buk_name_Area +' Empresa='+str(company_id)
                    
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************
                    #endif


                if Buk_FICHA!=FICHA_RelacionLaboral:
                    print('Diferencia en FICHA. SPI;',FICHA_RelacionLaboral,' BUK:',Buk_FICHA)
                if company_id!=ID_EMPRESA_RelacionLaboral:
                    #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                    # L   O   G   ****************************************************************
                    Actividad = 'Empresas diferentes. Se procesa una TRANSFERENCIA. BUK:'+str(company_id)+" SPI:"+str(ID_EMPRESA_RelacionLaboral)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************



                    # SE PROCEDE A COLOCAR FECHA DE RETIRO Y MOTIVO CDE EN LA RELACIUON LABORAL ACTUAL 
                    parametros = {
                        'id_empresa':ID_EMPRESA_RelacionLaboral,
                        'Buk_FICHA':FICHA_RelacionLaboral,
                        'Buk_F_INGRESO':Buk_F_INGRESO,
                        "id_finiquito": "CDE",
                        'Buk_USRACT' :Buk_USRACT,
                    }
                    consulta = """
                        UPDATE TA_RELACION_LABORAL SET F_RETIRO=TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD')- INTERVAL '1' DAY,ID_FINIQUITO=:id_finiquito, USRACT=:Buk_USRACT, FECACT=SYSDATE
                            WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND F_RETIRO IS NULL
                        """ 
                    cursor.execute(consulta, parametros)
                        # L   O   G   ****************************************************************
                    Actividad = "Se actualiza la fecha de retiro="+Buk_F_INGRESO+"-1, en la relación laboral actual. Empresa="+str(ID_EMPRESA_RelacionLaboral)+", Ficha="+str(FICHA_RelacionLaboral)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                    # SE DA FECHA DE EGRESO A LA RELACION PUESTO ANTERIOR
                    parametros = {
                        'id_empresa':ID_EMPRESA_RelacionLaboral,
                        'Buk_FICHA':FICHA_RelacionLaboral,
                        'Buk_F_INGRESO':Buk_F_INGRESO,
                        'Buk_USRACT' :Buk_USRACT,
                    }
                    consulta = """
                            UPDATE TA_RELACION_PUESTO SET FECHA_FIN=TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD')- INTERVAL '1' DAY, USRACT=:Buk_USRACT,   FECACT=SYSDATE
                            WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND FECHA_FIN IS NULL
                    """
                    cursor.execute(consulta, parametros)
                    # L   O   G   ****************************************************************
                    Actividad = "Se da de BAJA TA_RELACION_PUESTO para la emoresa: "+ID_EMPRESA_RelacionLaboral+", la ficha="+FICHA_RelacionLaboral+" con FECHA_FIN="+Buk_F_INGRESO+"-1 "
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))



                    #BUSCAR INFORMACION EN TA_RELACION_LABORAL PARA VALIDAR SI EXISTIO RELACIONA ANTERIOR CON ESA COMPAÑIA (SI ES ASI ENTONCES SE CREAR NUEVA FICHA) 
                    parametros = {
                         'id_empresa':company_id,
                         'Buk_NUM_IDEN':Buk_NUM_IDEN.replace('.', ''),
                     }
                    consulta = """
                        SELECT COUNT(*) AS CUENTA  
                        FROM TA_RELACION_LABORAL trl ,EO_PERSONA ep 
                        WHERE trl.ID_PERSONA  = ep.ID 
                        AND trl.ID_EMPRESA!='BA' AND trl.F_RETIRO IS NOT NULL AND ep.NUM_IDEN =:Buk_NUM_IDEN and trl.ID_EMPRESA = :id_empresa 
                    """
                    #print(consulta)
                    cursor.execute(consulta, parametros)

                    count_TA_RELACION_LABORAL = cursor.fetchone()[0]
                    if count_TA_RELACION_LABORAL==0:
                        #print('11')
                        Actividad = 'El colaborador tendrá una nueva relacion laboral manteniendo la misma ficha. Ficha:'+str(Buk_FICHA)
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************  
                    else:
                        #print('22')
                        # L   O   G   ****************************************************************
                        Actividad = 'El colaborador ya ha trabajado en esta nueva Empresa('+str(company_id)+'). Se va generar una nueva ficha.'
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************                       
                        # reservo el numero de ficha
                        sql_query = "SELECT * FROM correlativos"
                        cursorApiEmpleado.execute(sql_query)
                        results_correlativo = cursorApiEmpleado.fetchone()
                        Buk_FICHA=results_correlativo[0]
                        # incremento el numero de ficha
                        valor_actual = Buk_FICHA
                        numero_actual = int(valor_actual[2:])
                        nuevo_numero = numero_actual + 1
                        fichas = f"BK{nuevo_numero}"
                        sql_query = f"update public.correlativos set ficha = '{fichas}'"
                        cursorApiEmpleado.execute(sql_query)
                        # L   O   G   ****************************************************************
                        Actividad = 'Se reserva el correlativo de ficha. Nueva ficha:'+str(Buk_FICHA)
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************
                    #endif
                    # EVALUAR SI EXISTE EL CARGO EL CARGO EN LA NUEVA COMPAÑIA
                    parametros = {
                        'id_empresa':company_id,
                        'Buk_ID_CARGO':Buk_ID_CARGO,
                    }
                    consulta = """
                            SELECT * FROM EO_CARGO A
                            WHERE A.ID_EMPRESA = :id_empresa AND A.ID = :Buk_ID_CARGO
                    """
                    cursor.execute(consulta, parametros)
                    results_EO_CARGO = cursor.fetchone()
                    #print('results_EO_CARGO',results_EO_CARGO)
                    if results_EO_CARGO is  None or results_EO_CARGO[0] is  None or not results_EO_CARGO:
                        id_cambio='10017'  # PROMOCION
                        #print('NO EXISTE EO_CARGO SE DEBE CREAR')
                        # CREAR EL NUEVO CARGO+++++++++++++++++++++++++++++++++++++++++++++++++
                        values_eo_cargo = {   
                            'Buk_ID_EMPRESA' :company_id,
                            'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE,
                            'Buk_ID_CARGO':Buk_ID_CARGO,
                            'Buk_USRCRE' :Buk_USRCRE,
                        }  
                        sql_query = "INSERT INTO INFOCENT.EO_CARGO "+ \
                        "(      ID_EMPRESA, ID,  NOMBRE,  USRCRE,  FECCRE) "+ \
                        "VALUES(:Buk_ID_EMPRESA,  :Buk_ID_CARGO ,:Buk_ID_CARGO_NOMBRE, :Buk_USRCRE, SYSDATE)"
                        cursor.execute(sql_query,values_eo_cargo)
                        # L   O   G   ****************************************************************
                        Actividad = "Se crea en EO_CARGO el cargo Código="+str(Buk_ID_CARGO)+' Cargo='+Buk_ID_CARGO_NOMBRE +' Empresa='+str(company_id)
                    
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************
                    #endif
                     # COMO SE DEBE CREA UN NUEVO PUESTO DEBEMOS UBICAR EL PROXIMO ID PARA EL NUEVO PUESTO
                    parametros = {
                        'id_empresa':company_id,
                        'id_unidad':Buk_ID_UNIDAD,
                    }
                    consulta = """
                        SELECT NVL(MAX(id), 0) + 1  FROM EO_PUESTO ep 
                        WHERE ID_EMPRESA =:id_empresa
                        AND ID_UNIDAD=:id_unidad
                    """
                    cursor.execute(consulta, parametros)
                    resultados_nuevo_puesto = cursor.fetchone()   
                    Buk_ID_PUESTO=resultados_nuevo_puesto[0]
                    #print('Buk_ID_PUESTO',Buk_ID_PUESTO,parametros,resultados_nuevo_puesto)


                    values_eo_puesto = {   
                        'Buk_ID_EMPRESA' :company_id,
                        'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                        'Buk_ID_PUESTO':Buk_ID_PUESTO,
                        'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE_SR,
                        'Buk_ID_CARGO':Buk_ID_CARGO,
                        'Buk_F_INGRESO':Buk_F_INGRESO,
                        'Buk_USRCRE' :Buk_USRCRE,
                    }    
                    #print('values_eo_puesto',values_eo_puesto)              

                    # SE CREA EL NUEVO PUESTO
                    sql_query = "INSERT INTO INFOCENT.EO_PUESTO "+ \
                    "(ID_EMPRESA, ID_UNIDAD, ID, NOMBRE, ID_CARGO, FECHA_INI,FECHA_FIN,USRCRE, FECCRE) "+ \
                    "VALUES(:Buk_ID_EMPRESA, :Buk_ID_UNIDAD, :Buk_ID_PUESTO, :Buk_ID_CARGO_NOMBRE, :Buk_ID_CARGO,TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') ,SYSDATE+300000, :Buk_USRCRE, SYSDATE)"
                    
                    cursor.execute(sql_query,values_eo_puesto)
                    #print('2')
                    # L   O   G   ****************************************************************
                    Actividad = "Se crea un NUEVO EO_PUESTO. Puesto_ID="+str(Buk_ID_PUESTO)+", Puesto="+Buk_ID_CARGO_NOMBRE_SR+' en Empresa='+str(company_id)+", unidad="+Buk_ID_UNIDAD+" "
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************              
                    #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                else:
                    # L   O   G   ****************************************************************
                    Actividad = 'Empresas iguales. Se procesa una PROMOCIÓN. BUK:'+str(company_id)+" SPI:"+str(ID_EMPRESA_RelacionLaboral)
                    print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                    #print('Igual compañia BUK y SPI. Se procede a evaluar promoción.')
                    #print('LOCAIDADES:',ID_LOCALIDAD_RelacionLaboral,Buk_LOCALIDAD)
                    if ID_LOCALIDAD_RelacionLaboral != Buk_LOCALIDAD:
                        # SE ENCONTRO LOCALIDAD DIFERENTE Y SE CAMBIA EN SPI
                        parametros = {
                         'id_empresa':company_id,
                         'Buk_FICHA':Buk_FICHA,
                         'LOCALIDAD':Buk_LOCALIDAD,
                         'Buk_USRACT' :Buk_USRACT,
                        }
                        consulta = """
                            UPDATE TA_RELACION_LABORAL SET ID_LOCALIDAD=:LOCALIDAD, USRACT=:Buk_USRACT, FECACT=SYSDATE
                             WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND F_RETIRO IS NULL
                         """
                        cursor.execute(consulta, parametros)
                         # L   O   G   ****************************************************************
                        Actividad = "Se registra un cambio de LOCALIDAD. SPI="+str(ID_LOCALIDAD_RelacionLaboral)+", BUK="+str(Buk_LOCALIDAD)
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************
                    #BUSCAR LA FICHA DEL JEFE PARA VER SI HAY CAMBIOS EN BUK la cedula viene de Buk_BOSS=(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut"))[:30]
                    parametros = {
                    'rut_jefe':(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut")).replace('.', '')[:30],
                    }
                    consulta = """
                        SELECT ID_EMPRESA empresa,ficha 
                        FROM TA_RELACION_LABORAL trl ,EO_PERSONA ep 
                    WHERE trl.ID_PERSONA  = ep.ID  AND ID_EMPRESA!='BA' AND F_RETIRO IS NULL AND ep.NUM_IDEN =:rut_jefe
                    """
                    cursor.execute(consulta, parametros)
                    results_boss = cursor.fetchone()
                    Buk_ID_EMPRESA_BOSS=results_boss[0]
                    Buk_FICHA_JEFE=results_boss[1]
                    #print('FICHA_JEFE:',FICHA_JEFE_RelacionLaboral,Buk_FICHA_JEFE)
                    if FICHA_JEFE_RelacionLaboral!=Buk_FICHA_JEFE:
                        if company_id==Buk_ID_EMPRESA_BOSS:
                            # SE ENCONTRO FICHA SE JEFE DIFEERENTE Y SE CAMBIA EN SPI SOLO SI LAS EMPRESAS DEL EMPLEADO Y JEFE SON LAS MISMAS
                            Buk_FICHA_JEFE=results_boss[1]
                            parametros = {
                            'id_empresa':company_id,
                            'Buk_FICHA':Buk_FICHA,
                            'JEFE':Buk_FICHA_JEFE,
                            'Buk_USRACT' :Buk_USRACT,
                            }
                            consulta = """
                                UPDATE TA_RELACION_LABORAL SET FICHA_JEFE=:JEFE, USRACT=:Buk_USRACT, FECACT=SYSDATE
                                WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND F_RETIRO IS NULL
                            """
                            cursor.execute(consulta, parametros)
                            # L   O   G   ****************************************************************
                            Actividad = "Se registra un cambio en FICHA DE COACH. SPI="+FICHA_JEFE_RelacionLaboral+", BUK="+Buk_FICHA_JEFE
                            print(Actividad)
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####********************************************* 
                        else:
                            # L   O   G   ****************************************************************
                            Actividad = "El COACH con la ficha="+Buk_FICHA_JEFE+ ", existe en la empres="+Buk_ID_EMPRESA_BOSS+",  diferente a la empresa="+company_id+" del colaborador."
                            Estatus = "ERROR"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************    
                            Buk_FICHA_JEFE=' '         
                        #print('boss',Buk_ID_EMPRESA_BOSS,Buk_FICHA_JEFE)
                        #print('ojo',company_id,Buk_FICHA)
                    #endif 

                    # EVALUAR SI TIENE TA_HIST_CONTRATO_TRABAJO
                    Buk_ID_CONT_TRAB='TI' if  (dataEmpleado.get("data", []).get("current_job", {}).get("type_of_contract"))=='Indefinido' else 'TD'
                    parametros = {
                         'id_empresa':company_id,
                         'Buk_FICHA':Buk_FICHA,
                     }
                    consulta = """
                             SELECT * FROM TA_HIST_CONTRATO_TRABAJO A
                             WHERE A.ID_EMPRESA = :id_empresa AND A.FICHA = :Buk_FICHA AND A.FECHA_FIN IS NULL
                     """
                    cursor.execute(consulta, parametros)
                    results_TA_HIST_CONTRATO_TRABAJO= cursor.fetchone()
                    crear_contrato=True
                    if results_TA_HIST_CONTRATO_TRABAJO:
                        if results_TA_HIST_CONTRATO_TRABAJO[1]==Buk_ID_CONT_TRAB:
                            #no se crea el contrato porque  existe y no hay cambios
                            crear_contrato=False
                        else:
                            parametros = {
                                'id_empresa':company_id,
                                'Buk_FICHA':Buk_FICHA,
                                'Buk_F_INGRESO':Buk_F_INGRESO,
                                'Buk_USRACT' :Buk_USRACT,
                            }
                            consulta = """
                                    UPDATE TA_HIST_CONTRATO_TRABAJO SET FECHA_FIN=TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD')- INTERVAL '1' DAY, USRACT=:Buk_USRACT, FECACT=SYSDATE
                                    WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND FECHA_FIN IS NULL
                            """
                            cursor.execute(consulta, parametros)
                            #print('results_TA_HIST_CONTRATO_TRABAJO[1]',results_TA_HIST_CONTRATO_TRABAJO[1],Buk_ID_CONT_TRAB)

                            # L   O   G   ****************************************************************
                            Actividad = "Se da de BAJA TA_HIST_CONTRATO_TRABAJO para la ficha="+Buk_FICHA+" y el Buk_ID_CONT_TRAB="+results_TA_HIST_CONTRATO_TRABAJO[1]
                            print(Actividad)
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************
                    else:
                        # L   O   G   ****************************************************************
                        Actividad = "No se encuentra un TA_HIST_CONTRATO_TRABAJO para la ficha="+Buk_FICHA 
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))                      
                    if crear_contrato:
                        Buk_FECHA_INI_new=results_TA_HIST_CONTRATO_TRABAJO[6]
                        # PREPARANDO DATA TA_HIST_CONTRATO_TRABAJO
                        print('Buk_FECHA_INI',Buk_FECHA_INI_new)
                        values_TA_HIST_CONTRATO_TRABAJO = {   
                            'Buk_ID_EMPRESA' :company_id,
                            'Buk_ID_CONT_TRAB' : Buk_ID_CONT_TRAB,
                            'Buk_ID' : 1,
                            'Buk_FICHA' :Buk_FICHA,
                            'Buk_ID_PERSONA' : Buk_ID,
                            'Buk_NUM_CONTRATO' : '1',
                            #'Buk_FECHA_INI' :Buk_F_INGRESO,
                            'Buk_FECHA_INI' : Buk_FECHA_INI_new, # se cambia la logica por indicaciones de jeisa correo promocion del 21-mar-2025
                            'Buk_ID_CAMBIO' : '10011',
                            'Buk_USRCRE' :Buk_USRCRE,
                        }    

                        sql_query = "INSERT INTO TA_HIST_CONTRATO_TRABAJO "+ \
                        "(ID_EMPRESA, ID_CONT_TRAB, ID, FICHA, ID_PERSONA,NUM_CONTRATO, FECHA_INI, ID_CAMBIO, USRCRE, FECCRE) "+ \
                        "VALUES(:Buk_ID_EMPRESA, :Buk_ID_CONT_TRAB, :Buk_ID, :Buk_FICHA, :Buk_ID_PERSONA, :Buk_NUM_CONTRATO, TO_DATE(:Buk_FECHA_INI, 'YYYY-MM-DD') , "+ \
                        ":Buk_ID_CAMBIO ,:Buk_USRCRE,SYSDATE) "
                        cursor.execute(sql_query,values_TA_HIST_CONTRATO_TRABAJO)
                        # L   O   G   ****************************************************************
                        Actividad = "Se crea un contrato laboral en TA_HIST_CONTRATO_TRABAJO para la ficha="+Buk_FICHA+" y el Buk_ID_CONT_TRAB="+Buk_ID_CONT_TRAB
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************

                    ##VARIABLES A UTILIZAR EN AS DIFERENTES VALIDACIONES
                    #id_cambio=''
                    ##Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))
                    #Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
                    #if isinstance((dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo")), str):
                    #    Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))[:5]
                    #else:
                    #    Buk_ID_CARGO=int(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))
                    #    Buk_ID_CARGO=str(Buk_ID_CARGO)
                    #Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado")).upper()
                    #Buk_Nivel_de_Seniority=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Nivel_de_Seniority")).upper()
                    #Buk_ID_CARGO_NOMBRE_SR= Buk_ID_CARGO_NOMBRE+" "+Buk_Nivel_de_Seniority
                    #print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
                    #print('Buk_ID_CARGO',Buk_ID_CARGO)
                    #print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO_NOMBRE)
                    #print('Buk_Nivel_de_Seniority',Buk_Nivel_de_Seniority)
                    #print('Buk_ID_CARGO_NOMBRE_SR',Buk_ID_CARGO_NOMBRE_SR)

                    # EVALUAR SI TIENE RELACION PUESTO
                    parametros = {
                         'id_empresa':company_id,
                         'Buk_FICHA':Buk_FICHA,
                     }
                    consulta = """
                             SELECT * FROM TA_RELACION_PUESTO A
                             WHERE A.ID_EMPRESA = :id_empresa AND A.FICHA = :Buk_FICHA AND A.FECHA_FIN IS NULL
                     """
                    cursor.execute(consulta, parametros)
                    results_TA_RELACION_PUESTO = cursor.fetchone()
                    #print(results_TA_RELACION_PUESTO)
                    if not results_TA_RELACION_PUESTO:
                        # L   O   G   ****************************************************************
                        Actividad = "No existe TA_RELACION_PUESTO. Revisar inconsistencia para la ficha="+Buk_FICHA+", Empresa="+company_id
                        print(Actividad)
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####********************************************* 

                    
                    else:
                        # EVALUAR SI HAY CAMBIOS EN EL CARGO
                        parametros = {
                            'id_empresa':company_id,
                            'Buk_ID_CARGO':Buk_ID_CARGO,
                        }
                        consulta = """
                                SELECT * FROM EO_CARGO A
                                WHERE A.ID_EMPRESA = :id_empresa AND A.ID = :Buk_ID_CARGO
                        """
                        cursor.execute(consulta, parametros)
                        results_EO_CARGO = cursor.fetchone()
                        #print('results_EO_CARGO',results_EO_CARGO)
                        if results_EO_CARGO is  None or results_EO_CARGO[0] is  None or not results_EO_CARGO:
                            id_cambio='10017'  # PROMOCION
                            #print('NO EXISTE EO_CARGO SE DEBE CREAR')
                            # CREAR EL NUEVO CARGO+++++++++++++++++++++++++++++++++++++++++++++++++
                            values_eo_cargo = {   
                                'Buk_ID_EMPRESA' :company_id,
                                'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE,
                                'Buk_ID_CARGO':Buk_ID_CARGO,
                                'Buk_USRCRE' :Buk_USRCRE,
                            }  
                            sql_query = "INSERT INTO INFOCENT.EO_CARGO "+ \
                            "(      ID_EMPRESA, ID,  NOMBRE,  USRCRE,  FECCRE) "+ \
                            "VALUES(:Buk_ID_EMPRESA,  :Buk_ID_CARGO ,:Buk_ID_CARGO_NOMBRE, :Buk_USRCRE, SYSDATE)"
                            cursor.execute(sql_query,values_eo_cargo)
                            # L   O   G   ****************************************************************
                            Actividad = "Se crea en EO_CARGO el cargo Código="+str(Buk_ID_CARGO)+' Cargo='+Buk_ID_CARGO_NOMBRE
                        
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************

                        # CARGAMOS LA INFORMACION DEL PUESTO ACTUAL PARA COMPARAR UN POSIBLE CAMBIO DE SENIORITY
                        parametros = {
                            'id_empresa':company_id,
                            'Buk_ID_CARGO':Buk_ID_CARGO,
                            'Buk_ID_UNIDAD':Buk_ID_UNIDAD,
                            'id_puesto':results_TA_RELACION_PUESTO[4],
                        }
                        consulta = """
                                SELECT * FROM EO_PUESTO A 
                                WHERE A.ID_EMPRESA = :id_empresa AND A.ID = :id_puesto AND A.ID_UNIDAD = :Buk_ID_UNIDAD AND A.ID_CARGO= :Buk_ID_CARGO 
                        """
                        
                        cursor.execute(consulta, parametros)
                        results_EO_PUESTO = cursor.fetchone()
                        
                        #print(results_EO_PUESTO,parametros)
                        #NOMBRE_PUESTO_results_EO_PUESTO=results_EO_PUESTO[3]
                        if not results_EO_PUESTO:
                            #print('NO EXISTE EO_PUESTO SE DEBE CREAR')
                            id_cambio='10017'
                            if Buk_ID_UNIDAD!=results_TA_RELACION_PUESTO[3]:
                                if results_EO_CARGO is  None or results_EO_CARGO[0] is  None or not results_EO_CARGO:
                                    id_cambio='10017'  # PROMOCION
                                    Actividad = "Se identifica una promoción de UNIDAD. SPI="+results_TA_RELACION_PUESTO[3]+" y BUK="+Buk_ID_UNIDAD+" "
                                #else:
                                #    id_cambio='20010' # RECLASIFICACION
                                # L   O   G   ****************************************************************
                                #    Actividad = "Se identifica una re-clasificación de UNIDAD. SPI="+results_TA_RELACION_PUESTO[3]+" y BUK="+Buk_ID_UNIDAD+" "
                                # L   O   G   ****************************************************************
                                Estatus = "INFO"
                                fecha_actual = datetime.now()
                                consulta = "INSERT INTO public.log "+ \
                                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                                "VALUES(%s, %s, %s, %s, %s, %s)"
                                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        else:
                            #####*********************************************
                            if results_EO_PUESTO[3]!=Buk_ID_CARGO_NOMBRE_SR:
                                #print('Cambio de seniority')
                                id_cambio='10017'  # PROMOCION
                                #print(results_EO_PUESTO[3],Buk_ID_CARGO_NOMBRE_SR)
                                #print('Cambio de seniority')
                                # L   O   G   ****************************************************************
                                Actividad = "Se procesa una promoción por cambio de Senionity EO_PUESTO. SPI="+results_EO_PUESTO[3]+" y BUK="+Buk_ID_CARGO_NOMBRE_SR+" "
                                Estatus = "INFO"
                                fecha_actual = datetime.now()
                                consulta = "INSERT INTO public.log "+ \
                                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                                "VALUES(%s, %s, %s, %s, %s, %s)"
                                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************

                        if id_cambio!='':
                            # COMO SE DEBE CREA UN NUEVO PUESTO DEBEMOS UBICAR EL PROXIMO ID PARA EL NUEVO PUESTO
                            parametros = {
                                'id_empresa':company_id,
                                'id_unidad':Buk_ID_UNIDAD,
                            }
                            consulta = """
                                SELECT NVL(MAX(id), 0) + 1  FROM EO_PUESTO ep 
                                WHERE ID_EMPRESA =:id_empresa
                                AND ID_UNIDAD=:id_unidad
                            """
                            cursor.execute(consulta, parametros)
                            resultados_nuevo_puesto = cursor.fetchone()   
                            Buk_ID_PUESTO=resultados_nuevo_puesto[0]
                            #print('Buk_ID_PUESTO',Buk_ID_PUESTO,parametros,resultados_nuevo_puesto)


                            values_eo_puesto = {   
                                'Buk_ID_EMPRESA' :company_id,
                                'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                                'Buk_ID_PUESTO':Buk_ID_PUESTO,
                                'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE_SR,
                                'Buk_ID_CARGO':Buk_ID_CARGO,
                                'Buk_F_INGRESO':Buk_F_INGRESO,
                                'Buk_USRCRE' :Buk_USRCRE,
                            }    
                            #print('values_eo_puesto',values_eo_puesto)              

                            # SE CREA EL NUEVO SENIORITY
                            sql_query = "INSERT INTO INFOCENT.EO_PUESTO "+ \
                            "(ID_EMPRESA, ID_UNIDAD, ID, NOMBRE, ID_CARGO, FECHA_INI,FECHA_FIN,USRCRE, FECCRE) "+ \
                            "VALUES(:Buk_ID_EMPRESA, :Buk_ID_UNIDAD, :Buk_ID_PUESTO, :Buk_ID_CARGO_NOMBRE, :Buk_ID_CARGO,TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') ,SYSDATE+300000, :Buk_USRCRE, SYSDATE)"
                            
                            cursor.execute(sql_query,values_eo_puesto)
                            #print('2')
                            # L   O   G   ****************************************************************
                            Actividad = "Se crea un NUEVO EO_PUESTO. Puesto_ID="+str(Buk_ID_PUESTO)+", Puesto="+Buk_ID_CARGO_NOMBRE_SR+" "
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************
                        
                            # SE DA FECHA DE EGRESO A LA RELACION PUESTO ANTERIOR
                            parametros = {
                                'id_empresa':company_id,
                                'Buk_FICHA':Buk_FICHA,
                                'Buk_F_INGRESO':Buk_F_INGRESO,
                                'Buk_USRACT' :Buk_USRACT,
                            }
                            consulta = """
                                    UPDATE TA_RELACION_PUESTO SET FECHA_FIN=TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD')- INTERVAL '1' DAY, USRACT=:Buk_USRACT, FECACT=SYSDATE
                                    WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND FECHA_FIN IS NULL
                            """
                            cursor.execute(consulta, parametros)
                            # L   O   G   ****************************************************************
                            Actividad = "Se da de BAJA TA_RELACION_PUESTO para la ficha="+Buk_FICHA+" CON FECHA_FIN="+Buk_F_INGRESO+", Puesto_ID="+str(results_TA_RELACION_PUESTO[4])
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))

                            # SE CTREA UNA NUEVA RELACION PUESTO CON EL NUEVO PUESTO
                            values_ta_relacion_puesto = {   
                                    'Buk_ID_EMPRESA' :company_id,
                                    'Buk_FICHA' :Buk_FICHA,
                                    'Buk_F_INGRESO' :Buk_F_INGRESO,
                                    'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                                    'Buk_ID_PUESTO' :Buk_ID_PUESTO,
                                    'Buk_ID_CAMBIO':id_cambio,
                                    'Buk_USRCRE' :Buk_USRCRE, 
                                }  
                            sql_query = "INSERT INTO TA_RELACION_PUESTO "+ \
                            "(ID_EMPRESA, FICHA, FECHA_INI, ID_UNIDAD, ID_PUESTO,ID_CAMBIO, USRCRE, FECCRE) "+ \
                            "VALUES(:Buk_ID_EMPRESA, :Buk_FICHA, TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD'), :Buk_ID_UNIDAD, :Buk_ID_PUESTO,:Buk_ID_CAMBIO, :Buk_USRCRE, SYSDATE) "
                            #print(sql_query)
                            cursor.execute(sql_query,values_ta_relacion_puesto)
                            # L   O   G   ****************************************************************
                            Actividad = "Se crea una TA_RELACION_PUESTO. Puesto ID="+str(Buk_ID_PUESTO)
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    # #####********************************************
                #ENDIF
            #ENDIF
            Estatus = "1"
            fecha_actual = datetime.now()
            consulta = "UPDATE empleados set status_process=%s,date_process=%s where id=%s "
            cursorApiEmpleado.execute(consulta, (Estatus, fecha_actual,transacction_id))

            sql_query = 'SELECT * FROM log WHERE id_buk = \'' + str(transacction_id) + '\''

            cursorApiEmpleado.execute(sql_query)
            results_log = cursorApiEmpleado.fetchall()
            connectionPg.commit()

            # Crea el mensaje
            msg = MIMEMultipart()
            msg['Subject'] = 'Evento de Ficha Nro: ' +Buk_FICHA+' del tipo: '+event_type_id + '. Colaborador: ' + name_id + ' de fecha: ' + date_id
            msg['From'] = 'jcuauro@gmail.com'
            msg['To'] = 'jhidalgo@alfonzorivas.com'
            #msg['To'] = 'jcuauro@gmail.com'
            # Crea el cuerpo del mensaje
            cuerpo_mensaje = f"""Estimado/a ,

    Le informamos que se ha procesado el número de ficha {Buk_FICHA} para {name_id} , nro de Documento: {ci_id}

    **Detalles de proceso:**

    Fecha y Hora                        | Mensaje 
    |-----------------------------------|----------------------------------------------------------------------------------------------|
    """

            for row in results_log:
                cuerpo_mensaje += f"| {row[1]} | {row[5]} | {row[4]} |\n"

            cuerpo_mensaje += """


    Atentamente,

    Sistema Automático de gestión de Transferencias, Promociones y Re-clasificación.
    """
            msg.attach(MIMEText(cuerpo_mensaje, 'plain'))
                # Envía el correo electrónico
            server.sendmail('jcuauro@gmail.com', 'jhidalgo@alfonzorivas.com', msg.as_string())
            #server.sendmail('jcuauro@gmail.com', 'jcuauro@gmail.com', msg.as_string())


            connection.commit() # cAMBIOS EN SPI ORACLE
            #connection.rollback()
            connectionPg.commit()  #cambios en postgree LOG
            #connectionPg.rollback()



        #ENDFOR
        # Confirmar la transacción si no hubo errores
        
        #connection.commit()
        #connection.rollback()
        
        # LISTA EL CONTENIDO DEL LOG
        #sql_query = 'SELECT * FROM log WHERE id_buk = \'' + str(transacction_id) + '\''

        #cursorApiEmpleado.execute(sql_query)
        #results_log = cursorApiEmpleado.fetchall()
        #for row in results_log:
        #    print(row)
        
        #connectionPg.commit()
        #connectionPg.rollback()
        
        print("Transacción Finalizada")
    except cx_Oracle.DatabaseError as e:
        # Manejar excepciones relacionadas con la base de datos
        print("Error de base de datos:", e)
        # Realizar rollback en caso de error
        connection.rollback()
        print("Rollback realizado")
    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    cursorApiEmpleado.close()
    connectionPg.close()
    connection.close()
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
    connectionPg.rollback()
    print("Rollback realizado")
