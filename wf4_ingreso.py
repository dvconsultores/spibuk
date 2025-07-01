"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
    Este programa gestiona en INGRESO de colaboradores a SPI.
 
    Esto se realiza mediante la informacion de la tala workflow_alta de PostgreSQL.

    select * from public.workflow_alta where status_ingreso is not null and status_process is null
    Nota: esta logina de banderas lo controla el wf3_envio_correo.py

"""
import unicodedata
import cx_Oracle
import requests
import psycopg2
from datetime import datetime

#manejo de correos
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

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

email_user = os.getenv("EMAIL_USER")
email_pass = os.getenv("EMAIL_PASS")

dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_service)

def quitar_acentos(cadena):
    # Normalizar la cadena a la forma NFD (descomposición)
    cadena_normalizada = unicodedata.normalize('NFD', cadena)
    # Filtrar solo los caracteres ASCII
    cadena_sin_acentos = ''.join(
        char for char in cadena_normalizada
        if unicodedata.category(char) != 'Mn'
    )
    return cadena_sin_acentos

try:
    #  Configura la conexión al servidor SMTP
    server = smtplib.SMTP('smtp-relay.gmail.com', 587)

    #server = smtplib.SMTP('smtp.gmail.com', 587)
    #server.starttls()
    #server.login(email_user, email_pass)
    print("Conexión exitosa a smtp")


    ##*************************************** ORACLE SPI
    connection = cx_Oracle.connect(oracle_user, oracle_pass, dsn)
    print("Conexión exitosa a Oracle SPI")

    cursor = connection.cursor()
    ##*************************************** POSTGRESQL
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
        #sql_query = "SELECT * FROM empleados where event_type  ='employee_create' and status_process is null"
        sql_query = "select * from public.workflow_alta where status_ingreso is not null and status_process is null order by id"
        cursorApiEmpleado.execute(sql_query)
        results = cursorApiEmpleado.fetchall()
        employee_id=''
        for row in results:
            employee_id=row[4]
            transacction_id=row[0]
            #print('empleado json',employee_id,'',transacction_id)

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
                #print("El colaborador NO EXISTE")
                sql_query = "SELECT * FROM correlativos"
                cursorApiEmpleado.execute(sql_query)
                results_correlativo = cursorApiEmpleado.fetchone()
                Buk_ID=results_correlativo[1]
                #Buk_FICHA=results_correlativo[0] antes lo contralala la tabla correlativos ahora se reserva antes por el workflow
                Buk_FICHA=row[2]
                #print(Buk_ID)
                #print(Buk_FICHA)
                # L   O   G   ****************************************************************
                Actividad = "El colaborador NO EXISTE"
                print('Actividad',Actividad)
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
            else:
                #print(f"El colaborador EXISTE: {count_result}")
                sql_query = 'SELECT ID FROM EO_PERSONA WHERE NUM_IDEN = '+Buk_NUM_IDEN.replace('.', '')
                #print('sql_query',sql_query)
                cursor.execute(sql_query)
                results_correlativo = cursor.fetchone()
                Buk_ID=results_correlativo[0]
                sql_query = "SELECT * FROM correlativos"
                cursorApiEmpleado.execute(sql_query)
                results_correlativo = cursorApiEmpleado.fetchone()
                #Buk_FICHA=results_correlativo[0] antes lo contralala la tabla correlativos ahora se reserva antes por el workflow
                Buk_FICHA=row[2]
                print(Buk_ID)
                print(Buk_FICHA)
                # L   O   G   ****************************************************************
                Actividad = "El colaborador EXISTE"
                print('Actividad',Actividad)
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
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
                #else:
                    #print("No se encontró ningún elemento con ID igual a 10.")

            else:
                print("Error al realizar la solicitud GET a la API. :", responseEmpresa.status_code)

            #determina el id de la ENTIDAD FEDERAL
            Buk_ESTADO = (dataEmpleado.get("data", [])
                                .get("custom_attributes", {})
                                .get("Estado", "")
                                .strip()
                                .upper())
            #print('Buk_ESTADO:', Buk_ESTADO,quitar_acentos(Buk_ESTADO))
            

            parametros = {
                'Buk_ESTADO':quitar_acentos(Buk_ESTADO),
            }
            consulta = """
                    SELECT CODIGO FROM SPI_ENTIDAD_FEDERAL WHERE CODIGO_PAIS = 'VEN' AND UPPER(TRIM(NOMBRE)) = :Buk_ESTADO
            """
            #print(consulta)
            cursor.execute(consulta, parametros)
            #print('consulta',consulta,Buk_ESTADO)
            Buk_ID_ENTFE_NA = cursor.fetchone()[0]
            #print('Buk_ESTADO:',Buk_ESTADO,'Buk_ID_ENTFE_NA:',Buk_ID_ENTFE_NA)

            #DETERMINAR EL CODIGO DE LOCALIDAD
            Buk_SEDE=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Sede"))[:30]
            consulta = "Select  codloc from public.localidades where Sede=%s and CIA_CODCIA=%s  "
            #print('consulta localidades',consulta,Buk_SEDE,ID_EMPRESA)
            cursorApiEmpleado.execute(consulta, (Buk_SEDE, ID_EMPRESA))
            results = cursorApiEmpleado.fetchone()

            if results is None:
                Buk_SEDE_Estado=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Estado").upper())[:30]
                Buk_SEDE = f"%{Buk_SEDE}%"
                consulta = "SELECT codloc FROM public.localidades WHERE UPPER(Sede) LIKE %s AND CIA_CODCIA = %s AND UPPER(Estado) = %s"
                cursorApiEmpleado.execute(consulta, (Buk_SEDE, ID_EMPRESA, Buk_SEDE_Estado))
                #print(Buk_SEDE, ID_EMPRESA)
                results = cursorApiEmpleado.fetchone()
                if results is None:
                    # L   O   G   ****************************************************************
                    Actividad = 'Buk reporta la localidad: '+str(Buk_SEDE)+ 'y no existe en loclidades.'
                    #print(Actividad)
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
                    #print(Actividad)
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************                    
                    Buk_LOCALIDAD=results[0]
            else:
                Buk_LOCALIDAD=results[0]
            #print('Buk_LOCALIDAD',Buk_LOCALIDAD)

            Buk_NOMBRE=(dataEmpleado.get("data", []).get("first_name"))[:120]
            partes = Buk_NOMBRE.split(" ")
            if len(partes) >= 2:
                Buk_NOMBRE1 = partes[0]
                Buk_NOMBRE2 = partes[1]
            else:
                Buk_NOMBRE1=(dataEmpleado.get("data", []).get("first_name"))[:120]
            Buk_APELLIDO1=(dataEmpleado.get("data", []).get("surname"))[:17]
            Buk_APELLIDO2=(dataEmpleado.get("data", []).get("second_surname"))[:15]
            Buk_ID_TIPO_IDEN=1
            Buk_NACIONAL=(dataEmpleado.get("data", []).get("nationality"))[:50]
            Buk_NUM_IDEN=(dataEmpleado.get("data", []).get("rut")).replace('.', '')[:20]
            if Buk_LOCALIDAD !='999':
                #print('Buk_NUM_IDEN',Buk_NUM_IDEN)
                Buk_PASAPORTE=(dataEmpleado.get("data", []).get("rut")).replace('.', '')[:10]
                #Buk_FECHA_NA=datetime.strptime((dataEmpleado.get("data", []).get("birthday")), '%Y-%m-%d')
                Buk_FECHA_NA=(dataEmpleado.get("data", []).get("birthday"))
                Buk_CIUDAD_NA=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Ciudad"))[:30]
                #Buk_ID_ENTFE_NA=Buk_LOCALIDAD
                if (dataEmpleado.get("data", []).get("country_code"))=='VE':
                    Buk_ID_PAIS_NA='VEN'
                Buk_SEXO='1' if dataEmpleado.get("data", []).get("gender")=='M' else '2'          
                #Buk_EDO_CIVIL=(dataEmpleado.get("data", []).get("civil_status"))[:0].upper()
                Buk_EDO_CIVIL = (dataEmpleado.get("data", {}).get("civil_status", "")[:1] or "").upper()
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
                Buk_TELEFONO1=(dataEmpleado.get("data", []).get("office_phone"))[:120]
                Buk_TELEFONO2=' '
                Buk_FAX=' '
                Buk_CELULAR=(dataEmpleado.get("data", []).get("phone"))[:120]

                if dataEmpleado.get("data", []).get("email"):
                    Buk_E_MAIL1=(dataEmpleado.get("data", []).get("email"))[:120]
                else:
                    Buk_E_MAIL1=''

                if dataEmpleado.get("data", []).get("personal_email"):
                    Buk_E_MAIL2=(dataEmpleado.get("data", []).get("personal_email"))[:120]
                else:
                    Buk_E_MAIL2=''
                Buk_IN_REL_TRAB=''
                Buk_USRCRE='ETL'
                #----
                #print('Buk_FECHA_NA',Buk_FECHA_NA) #TO_DATE(:fecha_na, 'YYYY-MM-DD') 

                if count_result == 0:         
                    values_eo_persona = {
                    'Buk_ID' :Buk_ID,
                    'Buk_NOMBRE1' :Buk_NOMBRE1,
                    'Buk_NOMBRE2' :Buk_NOMBRE2,
                    'Buk_APELLIDO1' :Buk_APELLIDO1,
                    'Buk_APELLIDO2' :Buk_APELLIDO2,
                    'Buk_ID_TIPO_IDEN' :Buk_ID_TIPO_IDEN,
                    'Buk_NACIONAL' :Buk_NACIONAL,
                    'Buk_NUM_IDEN' :Buk_NUM_IDEN,
                    'Buk_PASAPORTE' : Buk_PASAPORTE,
                    'Buk_FECHA_NA' :Buk_FECHA_NA,
                    'Buk_CIUDAD_NA' :Buk_CIUDAD_NA,
                    'Buk_ID_ENTFE_NA' :Buk_ID_ENTFE_NA,
                    'Buk_ID_PAIS_NA' :Buk_ID_PAIS_NA,
                    'Buk_SEXO' :Buk_SEXO,
                    'Buk_EDO_CIVIL' :Buk_EDO_CIVIL,
                    'Buk_ZURDO'  :Buk_ZURDO,
                    'Buk_TIPO_SANGRE' :Buk_TIPO_SANGRE,
                    'Buk_FACTOR_RH' :Buk_FACTOR_RH,
                    'Buk_DIRECCION' :Buk_DIRECCION,
                    'Buk_CIUDAD' :Buk_CIUDAD,
                    'Buk_ID_ENTFE' :Buk_ID_ENTFE,
                    'Buk_ID_PAIS' :Buk_ID_PAIS,
                    'Buk_PARROQUIA' :Buk_PARROQUIA,
                    'Buk_MUNICIPIO' :Buk_MUNICIPIO,
                    'Buk_COD_POSTAL' :Buk_COD_POSTAL,
                    'Buk_TELEFONO1' :Buk_TELEFONO1,
                    'Buk_TELEFONO2' :Buk_TELEFONO2,
                    'Buk_FAX' :Buk_FAX,
                    'Buk_CELULAR' :Buk_CELULAR,
                    'Buk_E_MAIL1' :Buk_E_MAIL1,
                    'Buk_E_MAIL2' :Buk_E_MAIL2,
                    'Buk_IN_REL_TRAB' :Buk_IN_REL_TRAB,
                    'Buk_USRCRE' :Buk_USRCRE,
                    'ENFERMEDADOCU' : '0',
                    'ETNIAINDIGENA' :'0',
                    'DISCAUDITIVA' :'0',
                    'DISCVISUAL' :'0',
                    'DISCINTELECTUAL' :'0',
                    'DISCMENTAL' :'0',
                    'DISCMUSCULOESQ' :'0',
                    'DISCACCIDENTE' :'0',
                    'DISCOTRA' :'0',
                    }
                    sql_query = "INSERT INTO EO_PERSONA "+ \
                    "(ID, NOMBRE1, NOMBRE2, APELLIDO1, APELLIDO2, ID_TIPO_IDEN, NACIONAL, NUM_IDEN, PASAPORTE, "+ \
                    "FECHA_NA, CIUDAD_NA, ID_ENTFE_NA, ID_PAIS_NA, SEXO, EDO_CIVIL, ZURDO, TIPO_SANGRE, FACTOR_RH, "+ \
                    "DIRECCION, CIUDAD, ID_ENTFE, ID_PAIS, PARROQUIA, MUNICIPIO, COD_POSTAL, TELEFONO1, TELEFONO2, FAX, "+ \
                    "CELULAR, E_MAIL1, E_MAIL2, IN_REL_TRAB, USRCRE, FECCRE, "+ \
                    "USRACT, FECACT, NOMBRE_FOTO, ENFERMEDADOCU, ETNIAINDIGENA, DISCAUDITIVA, DISCVISUAL, DISCINTELECTUAL, DISCMENTAL, DISCMUSCULOESQ, DISCACCIDENTE, DISCOTRA, DESCRIDISCA) "+ \
                    "VALUES (:Buk_ID, UPPER(:Buk_NOMBRE1), UPPER(:Buk_NOMBRE2), UPPER(:Buk_APELLIDO1), UPPER(:Buk_APELLIDO2), :Buk_ID_TIPO_IDEN, :Buk_NACIONAL, :Buk_NUM_IDEN, :Buk_PASAPORTE, "+ \
                    "TO_DATE(:Buk_FECHA_NA, 'YYYY-MM-DD'), :Buk_CIUDAD_NA, :Buk_ID_ENTFE_NA, :Buk_ID_PAIS_NA, :Buk_SEXO, :Buk_EDO_CIVIL, :Buk_ZURDO , :Buk_TIPO_SANGRE, :Buk_FACTOR_RH, "+ \
                    ":Buk_DIRECCION, :Buk_CIUDAD, :Buk_ID_ENTFE, :Buk_ID_PAIS, :Buk_PARROQUIA, :Buk_MUNICIPIO, :Buk_COD_POSTAL, :Buk_TELEFONO1, :Buk_TELEFONO2, :Buk_FAX, "+ \
                    ":Buk_CELULAR, :Buk_E_MAIL1, :Buk_E_MAIL2, :Buk_IN_REL_TRAB, :Buk_USRCRE, SYSDATE, "+ \
                    "'', '', '', :ENFERMEDADOCU, :ETNIAINDIGENA, :DISCAUDITIVA, :DISCVISUAL, :DISCINTELECTUAL, :DISCMENTAL, :DISCMUSCULOESQ, :DISCACCIDENTE, :DISCOTRA, '')"
                    cursor.execute(sql_query,values_eo_persona)
                    sql_query = "select * from  EO_PERSONA where ID="+Buk_ID
                    cursor.execute(sql_query)
                    results_empleado = cursor.fetchone()
                    #print (results_empleado)
                    # L   O   G   ****************************************************************
                    Actividad = "Se crea un nuevo colaborador en EO_PERSONA"
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
                # PREPARANDO DATA TA_RELACION_LABORAL 
                Buk_ID_EMPRESA=ID_EMPRESA
                Buk_ID_PERSONA=Buk_ID
                #Buk_F_INGRESO = datetime.strptime(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"), '%Y-%m-%d').strftime('%Y-%m-%d')
                #Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))
                Buk_F_INGRESO=(dataEmpleado.get("data", []).get("active_since"))
                Buk_F_CORPORACION=Buk_F_INGRESO
                Buk_F_AJUSTADA1=Buk_F_INGRESO
                Buk_F_AJUSTADA2=Buk_F_INGRESO
                Buk_F_AJUSTADA3=Buk_F_INGRESO
                Buk_ID_LOCALIDAD=Buk_LOCALIDAD
                Buk_ID_CATEGORIA1=''
                Buk_ID_CATEGORIA2=''
                Buk_ID_CATEGORIA3=''
                Buk_ID_SINDICATO=''
                Buk_ID_CENTRO_MED=''
                Buk_NRO_RIF= (dataEmpleado.get("data", []).get("custom_attributes", {}).get("RIF"))
                Buk_ID_FINIQUITO=''
                Buk_NRO_SSO=''
                #BUSCAR LA FICHA DEL JEFE la cedula viene de Buk_BOSS=(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut"))[:30]
                sql_query = "SELECT ID_EMPRESA empresa,ficha " + \
                "FROM TA_RELACION_LABORAL trl ,EO_PERSONA ep "+ \
                "WHERE trl.ID_PERSONA  = ep.ID " + \
                "AND ID_EMPRESA!='BA' AND F_RETIRO IS NULL AND ep.NUM_IDEN ="+(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut")).replace('.', '')[:30]
                cursor.execute(sql_query)
                results_boss = cursor.fetchone()
                #print('results_boss',results_boss,(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut")).replace('.', '')[:30])

                if results_boss is  None or results_boss[0] is  None:
                    # L   O   G   ****************************************************************
                    Actividad = "No se logra ubicar al COACH , se procede a ASIGNAR en la Buk_FICHA_JEFE=Buk_FICHA_COLABORADOR ."
                    Estatus = "ERROR"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    Buk_FICHA_JEFE=Buk_FICHA 
                else:
                    
                    Buk_ID_EMPRESA_BOSS=results_boss[0]
                    Buk_FICHA_JEFE=results_boss[1]
                    if Buk_ID_EMPRESA==Buk_ID_EMPRESA_BOSS:
                        Buk_FICHA_JEFE=results_boss[1]
                    else:
                        # L   O   G   ****************************************************************
                        Actividad = "El COACH con la ficha: "+Buk_FICHA_JEFE+ ", existe en la empresa: "+Buk_ID_EMPRESA_BOSS+", diferente a la empresa: "+Buk_ID_EMPRESA+" del colaborador. , se procede a ASIGNAR en la Buk_FICHA_JEFE=Buk_FICHA_COLABORADOR ."
                        Estatus = "ERROR"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************    
                        #Buk_FICHA_JEFE=' '   # se cambia la logica si los valores no son validos se le asigna la ficha de jefe la misma ficha del colaborador     
                        Buk_FICHA_JEFE=Buk_FICHA 
                    #print('boss',Buk_ID_EMPRESA_BOSS,Buk_FICHA_JEFE)
                    #print('ojo',Buk_ID_EMPRESA,Buk_FICHA)

                values_ta_relacion_laboral = {   
                    'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    'Buk_FICHA' :Buk_FICHA,
                    'Buk_ID_PERSONA' :Buk_ID_PERSONA,
                    'Buk_F_INGRESO' :Buk_F_INGRESO,
                    'Buk_F_CORPORACION' :Buk_F_CORPORACION,
                    'Buk_F_AJUSTADA1' :Buk_F_AJUSTADA1,
                    'Buk_F_AJUSTADA2' :Buk_F_AJUSTADA2,
                    'Buk_F_AJUSTADA3' :Buk_F_AJUSTADA3,
                    'Buk_ID_LOCALIDAD' :Buk_ID_LOCALIDAD,
                    'Buk_NRO_RIF' :'V'+str(Buk_NRO_RIF),
                    'Buk_NRO_SSO' :Buk_NRO_SSO,
                    'Buk_USRCRE' :Buk_USRCRE,
                    'Buk_FICHA_JEFE' :Buk_FICHA_JEFE,
                }    


                sql_query = "INSERT INTO TA_RELACION_LABORAL "+ \
                "(ID_EMPRESA, FICHA, ID_PERSONA, F_INGRESO, F_CORPORACION, F_AJUSTADA1, "+ \
                "F_AJUSTADA2, F_AJUSTADA3, ID_LOCALIDAD, NRO_RIF, NRO_SSO,  USRCRE, FECCRE, FICHA_JEFE) "+ \
                "VALUES(:Buk_ID_EMPRESA, :Buk_FICHA, :Buk_ID_PERSONA, TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') , TO_DATE(:Buk_F_CORPORACION, 'YYYY-MM-DD') , TO_DATE(:Buk_F_AJUSTADA1, 'YYYY-MM-DD') , "+ \
                " TO_DATE(:Buk_F_AJUSTADA2, 'YYYY-MM-DD'), TO_DATE(:Buk_F_AJUSTADA3, 'YYYY-MM-DD'), :Buk_ID_LOCALIDAD, :Buk_NRO_RIF, :Buk_NRO_SSO, "+ \
                ":Buk_USRCRE,SYSDATE,:Buk_FICHA_JEFE) "
                cursor.execute(sql_query,values_ta_relacion_laboral)
                # L   O   G   ****************************************************************
                Actividad = "Se crea una relación laboral en TA_RELACION_LABORAL"
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####********************************************* 20241004
                # PREPARANDO DATA TA_HIST_CONTRATO_TRABAJO
                Buk_ID_CONT_TRAB=(dataEmpleado.get("data", []).get("current_job", {}).get("type_of_contract"))

                values_TA_HIST_CONTRATO_TRABAJO = {   
                    'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    'Buk_ID_CONT_TRAB' : 'TI' if Buk_ID_CONT_TRAB=='Indefinido' else 'TD',
                    'Buk_ID' : 1,
                    'Buk_FICHA' :Buk_FICHA,
                    'Buk_ID_PERSONA' : Buk_ID_PERSONA,
                    'Buk_NUM_CONTRATO' : '1',
                    'Buk_FECHA_INI' :Buk_F_INGRESO,
                    'Buk_ID_CAMBIO' : '10001',
                    'Buk_USRCRE' :Buk_USRCRE,
                }    

                sql_query = "INSERT INTO TA_HIST_CONTRATO_TRABAJO "+ \
                "(ID_EMPRESA, ID_CONT_TRAB, ID, FICHA, ID_PERSONA,NUM_CONTRATO, FECHA_INI, ID_CAMBIO, USRCRE, FECCRE) "+ \
                "VALUES(:Buk_ID_EMPRESA, :Buk_ID_CONT_TRAB, :Buk_ID, :Buk_FICHA, :Buk_ID_PERSONA, :Buk_NUM_CONTRATO, TO_DATE(:Buk_FECHA_INI, 'YYYY-MM-DD') , "+ \
                ":Buk_ID_CAMBIO ,:Buk_USRCRE,SYSDATE) "
                cursor.execute(sql_query,values_TA_HIST_CONTRATO_TRABAJO)
                # L   O   G   ****************************************************************
                Actividad = "Se crea un contrato laboral en TA_HIST_CONTRATO_TRABAJO"
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))


                #####*********************************************
                # PREPARANDO DATA TA_RELACION_PUESTO 
                Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
                if isinstance((dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo")), str):
                    Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))[:5]
                else:
                    Buk_ID_CARGO=int(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))
                    Buk_ID_CARGO=str(Buk_ID_CARGO)         
                Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado")).upper()
                # SE INTENTA REUTILIZAR UN ID DE CARGO QUE ESTE DISPONIBLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!     Buk_ID_EMPRESA
                parametros = {
                    'id_empresa':Buk_ID_EMPRESA,
                    'Buk_ID_CARGO':Buk_ID_CARGO,
                }
                consulta = """
                        SELECT * FROM EO_CARGO ep 
                        WHERE ID_EMPRESA = :id_empresa AND ID = :Buk_ID_CARGO
                """
                cursor.execute(consulta, parametros)
                resultados = cursor.fetchone()
                #print('resultados CARGO',resultados)
                if resultados is  None or resultados[0] is  None:
                    #print('no existe el cargo')
                    # CREAR EL NUEVO CARGO+++++++++++++++++++++++++++++++++++++++++++++++++
                    values_eo_cargo = {   
                        'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                        'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE,
                        'Buk_ID_CARGO':Buk_ID_CARGO,
                        'Buk_USRCRE' :Buk_USRCRE,
                    }  
                    sql_query = "INSERT INTO INFOCENT.EO_CARGO "+ \
                    "(      ID_EMPRESA, ID,  NOMBRE,  USRCRE,  FECCRE) "+ \
                    "VALUES(:Buk_ID_EMPRESA,  :Buk_ID_CARGO ,UPPER(:Buk_ID_CARGO_NOMBRE), :Buk_USRCRE, SYSDATE)"
                    cursor.execute(sql_query,values_eo_cargo)
                    # L   O   G   ****************************************************************
                    Actividad = "Se crea un EO_CARGO "
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                # SE INTENTA REUTILIZAR UN ID DE CARGO QUE ESTE DISPONIBLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!     Buk_ID_EMPRESA
                parametros = {
                    'id_empresa':Buk_ID_EMPRESA,
                    'id_unidad':Buk_ID_UNIDAD,
                    'Buk_ID_CARGO':Buk_ID_CARGO,
                }
                consulta = """
                    SELECT min(id) FROM (
                        SELECT ID FROM EO_PUESTO ep 
                        WHERE ID_EMPRESA = :id_empresa AND ID_UNIDAD = :id_unidad and ID_CARGO = :Buk_ID_CARGO
                        MINUS
                        -- PUESTOS QUE ESTÁ SIENDO OCUPADOS PARA ESE CARGO
                        SELECT A.ID_PUESTO FROM TA_RELACION_PUESTO A, EO_PUESTO B
                        WHERE A.ID_EMPRESA = :id_empresa AND A.ID_UNIDAD = :id_unidad AND A.FECHA_FIN IS NULL
                        AND A.ID_EMPRESA = B.ID_EMPRESA AND A.ID_UNIDAD = B.ID_UNIDAD AND  B.ID_CARGO=:Buk_ID_CARGO
                        AND A.ID_PUESTO = B.ID 
                    )
                """
                cursor.execute(consulta, parametros)
                resultados = cursor.fetchone()
                #print('resultados',resultados)
                Buk_Nivel_de_Seniority=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Nivel_de_Seniority")).upper()
                if resultados is  None or resultados[0] is  None:
                    # DETERMINAR EL PROXIMO ID PARA EL PUESTO ++++++++++++++++++++++++++++++
                    parametros = {
                        'id_empresa':Buk_ID_EMPRESA,
                        'id_unidad':Buk_ID_UNIDAD,
                    }
                    #print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
                    #print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)

                    consulta = """
                        SELECT NVL(MAX(id), 0) + 1  FROM EO_PUESTO ep 
                        WHERE ID_EMPRESA =:id_empresa
                        AND ID_UNIDAD=:id_unidad
                    """
                    cursor.execute(consulta, parametros)
                    resultados_nuevo_puesto = cursor.fetchone()   
                    Buk_ID_PUESTO=resultados_nuevo_puesto[0]   
                    #print('Buk_ID_PUESTO',Buk_ID_PUESTO,'Buk_ID_CARGO:',Buk_ID_CARGO,'Buk_ID_UNIDAD:',Buk_ID_UNIDAD)
                    # CREAR EL NUEVO PUESTO+++++++++++++++++++++++++++++++++++++++++++++++++
                    values_eo_puesto = {   
                        'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                        'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                        'Buk_ID_PUESTO':Buk_ID_PUESTO,
                        'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE+" "+Buk_Nivel_de_Seniority,
                        'Buk_ID_CARGO':Buk_ID_CARGO,
                        'Buk_F_INGRESO':Buk_F_INGRESO,
                        'Buk_USRCRE' :Buk_USRCRE,
                    }  
                    sql_query = "INSERT INTO INFOCENT.EO_PUESTO "+ \
                    "(ID_EMPRESA, ID_UNIDAD, ID, NOMBRE, ID_CARGO, FECHA_INI, FECHA_FIN, USRCRE, FECCRE) "+ \
                    "VALUES(:Buk_ID_EMPRESA, :Buk_ID_UNIDAD, :Buk_ID_PUESTO, :Buk_ID_CARGO_NOMBRE, :Buk_ID_CARGO,TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') , SYSDATE+300000, :Buk_USRCRE, SYSDATE)"
                    cursor.execute(sql_query,values_eo_puesto)
                    # L   O   G   ****************************************************************
                    Actividad = "Se crea un NUEVO EO_PUESTO. id :"+str(Buk_ID_PUESTO)+" ( "+Buk_ID_CARGO_NOMBRE+" )"
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                else:
                    Buk_ID_PUESTO=resultados[0]
                    # L   O   G   ****************************************************************
                    Actividad = "Se encontro el Id  :"+str(Buk_ID_PUESTO)+" disponible en EO_PUESTO."
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####*********************************************
                #print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO,Buk_ID_CARGO_NOMBRE)
                #print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
                #print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
                #print('Buk_ID_PUESTO',Buk_ID_PUESTO)
                values_ta_relacion_puesto = {   
                    'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    'Buk_FICHA' :Buk_FICHA,
                    'Buk_F_INGRESO' :Buk_F_INGRESO,
                    'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                    'Buk_ID_PUESTO' :Buk_ID_PUESTO,
                    'Buk_ID_CAMBIO':'10001',
                    'Buk_USRCRE' :Buk_USRCRE, 
                }  
                sql_query = "INSERT INTO TA_RELACION_PUESTO "+ \
                "(ID_EMPRESA, FICHA, FECHA_INI, ID_UNIDAD, ID_PUESTO,ID_CAMBIO, USRCRE, FECCRE) "+ \
                "VALUES(:Buk_ID_EMPRESA, :Buk_FICHA, TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD'), :Buk_ID_UNIDAD, :Buk_ID_PUESTO,:Buk_ID_CAMBIO, :Buk_USRCRE, SYSDATE) "
                #print(sql_query)
                cursor.execute(sql_query,values_ta_relacion_puesto)
                # L   O   G   ****************************************************************
                Actividad = "Se crea una TA_RELACION_PUESTO "
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####********************************************
                """
                # PREPARANDO DATA NM_RELACION_PAGO
                Buk_NOMINA=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Nómina"))[:4]
                print('Buk_NOMINA',Buk_NOMINA)
                values_nm_relacion_pago = {   
                    'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    'Buk_FICHA' :Buk_FICHA,
                    'Buk_NOMINA' :Buk_NOMINA,
                    'Buk_USRCRE' :Buk_USRCRE, 
                }  
                sql_query = "INSERT INTO INFOCENT.NM_RELACION_PAGO "+ \
                "(ID_EMPRESA, FICHA, ID_NOMINA, USRCRE, FECCRE) "+ \
                "VALUES(:Buk_ID_EMPRESA, :Buk_FICHA, :Buk_NOMINA, :Buk_USRCRE, SYSDATE) "
                cursor.execute(sql_query,values_nm_relacion_pago)
                # L   O   G   ****************************************************************
                Actividad = "Se crea una NM_RELACION_PAGO , Nómina: "+Buk_NOMINA
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
                Buk_SUELDO=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Sueldo Básico (R)"))
                print('Buk_SUELDO',Buk_SUELDO)
                Buk_SUEDIA=round(Buk_SUELDO/30,2)
                print('Buk_SUEDIA',Buk_SUEDIA)
                # PREPARANDO DATA NMM004 (historial de nomina)
                values_nmm004 = {   
                    'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    'Buk_TRAB_SUBTIP' :'2',
                    'Buk_FICHA' :Buk_FICHA,
                    'Buk_TIPSUE':'1',
                    'Buk_F_INGRESO' :Buk_F_INGRESO,
                    'Buk_SUEDIA':Buk_SUEDIA,
                    'Buk_RCAM_CODCAM':'10001',
                    'Buk_USRCRE' :Buk_USRCRE, 
                } 
                sql_query = "INSERT INTO INFOCENT.NMM004 "+ \
                "(CIA_CODCIA, TRAB_SUBTIP, TRAB_FICTRA, TIPSUE, FECAUM, CONSEC, SUEDIA, RCAM_CODCAM,USRCRE, FECCRE) "+ \
                "VALUES(:Buk_ID_EMPRESA,:Buk_TRAB_SUBTIP,:Buk_FICHA,:Buk_TIPSUE,TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD'),SEC_HSUE.nextval ,:Buk_SUEDIA,:Buk_RCAM_CODCAM,:Buk_USRCRE,SYSDATE) "
                cursor.execute(sql_query,values_nmm004)
                # L   O   G   ****************************************************************
                Actividad = "Se crea una NMM004 , Sueldo Diario: "+str(Buk_SUEDIA)
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #*****************************************************************************
                """
                sql_query = 'SELECT * FROM log WHERE id_buk = \'' + str(transacction_id) + '\''

                cursorApiEmpleado.execute(sql_query)
                results_log = cursorApiEmpleado.fetchall()
                connectionPg.commit()
                msg = MIMEMultipart()
                msg['Subject'] = 'Notificación de registro de Ingreso, Ficha Nro: ' + Buk_FICHA + '. Colaborador: ' + Buk_NOMBRE1 + ' ' + Buk_APELLIDO1
                msg['From'] = email_user
                msg['To'] = 'jhidalgo@alfonzorivas.com'
                # Crea el cuerpo del mensaje
                cuerpo_mensaje = f"""Estimado/a ,

Le informamos que se ha procesado el ingreso del número de ficha {Buk_FICHA} para {Buk_NOMBRE1} {Buk_APELLIDO1}, nro de Documento: {Buk_NUM_IDEN}

    **Detalles de proceso:**

    Fecha y Hora                        | Mensaje 
    |-----------------------------------|----------------------------------------------------------------------------------------------|
    """

                for row in results_log:
                    cuerpo_mensaje += f"| {row[1]} | {row[5]} | {row[4]} |\n"

                cuerpo_mensaje += """

Atentamente,

Sistema Automático de gestión de ingresos.
                """
                msg.attach(MIMEText(cuerpo_mensaje, 'plain'))
                # Envía el correo electrónico
                server.sendmail(email_user, 'jhidalgo@alfonzorivas.com', msg.as_string())


                Estatus = "1"
                fecha_actual = datetime.now()
                consulta = "UPDATE public.workflow_alta set status_process=%s,date_process=%s where id=%s "
                cursorApiEmpleado.execute(consulta, (Estatus, fecha_actual,transacction_id))
                if count_result == 0:
                    #valor_actual = Buk_FICHA
                    #numero_actual = int(valor_actual[2:])
                    #nuevo_numero = numero_actual + 1
                    #ficha = f"BK{nuevo_numero}"

                    id_empleado = int(Buk_ID)+1
                    #consulta = "UPDATE correlativos set ficha=%s,id_empleado=%s " antes la ficha lo contralaba la tabla este proceso ahora
                    #cursorApiEmpleado.execute(consulta, (ficha, id_empleado))     lo controla wf2_envio_correo
                    consulta = "UPDATE correlativos set id_empleado="+str(id_empleado)
                    cursorApiEmpleado.execute(consulta)
            else:
                msg = MIMEMultipart()
                msg['Subject'] = 'Notificación de ERROR en LOCALIDAD para el registro de Ingreso, Ficha Nro: ' + Buk_FICHA + '. Colaborador: ' + Buk_NOMBRE1 + ' ' + Buk_APELLIDO1
                msg['From'] = email_user
                msg['To'] = 'jhidalgo@alfonzorivas.com'
                # Crea el cuerpo del mensaje
                cuerpo_mensaje = f"""Estimado/a ,

Le informamos que se ha encontrado un error al intenrar ubicar la localidad del colaborador.
La sede= {Buk_SEDE} y compañia {ID_EMPRESA} no están en la tabla de localidades para el número de ficha {Buk_FICHA} para {Buk_NOMBRE1} {Buk_APELLIDO1}, nro de Documento: {Buk_NUM_IDEN}
El ingreso no se puede realizar hasta que se corrija la inconsistencia.


Atentamente,

Sistema Automático de gestión de ingresos.
                """
                msg.attach(MIMEText(cuerpo_mensaje, 'plain'))
                # Envía el correo electrónico
                server.sendmail(email_user, 'jhidalgo@alfonzorivas.com', msg.as_string())


                Estatus = "3" # error de inconsstencia en la tabla public.localidades. No existe la sede-compañia de buk en la tabla
                fecha_actual = datetime.now()
                consulta = "UPDATE public.workflow_alta set status_process=%s,date_process=%s where id=%s "
                cursorApiEmpleado.execute(consulta, (Estatus, fecha_actual,transacction_id))               


        #ENDFOR
        # Confirmar la transacción si no hubo errores
        #connection.commit()
        connection.rollback()
        
        # LISTA EL CONTENIDO DEL LOG
        #sql_query = "SELECT * FROM log"
        #cursorApiEmpleado.execute(sql_query)
        #results_log = cursorApiEmpleado.fetchall()
        #for row in results_log:
        #    print(row)
        
        #connectionPg.commit()
        connectionPg.rollback()
        print("Transacción finalizada")
    except cx_Oracle.DatabaseError as e:
        # Manejar excepciones relacionadas con la base de datos
        print("Error de base de datos:", e)
        # Realizar rollback en caso de error
        #connection.rollback()
        print("Rollback realizado")
    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    cursorApiEmpleado.close()
    connectionPg.close()
    connection.close()
    server.quit()
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
