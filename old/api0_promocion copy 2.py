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


# credenciales ORACLE
username = 'INFOCENT'
password = 'M4NZ4N1LL4'
host = '192.168.254.201'
port = 1521
service_name = 'spitest'

dsn = cx_Oracle.makedsn(host, port, service_name)


#credenciales postgresql
dbnamePg = "spibuk"
userPg = "postgres"
passwordPg = "Q84Z7zQ2kR0WamnV4r6RLpWYhdD8JwDX"
hostPg = "64.225.104.69"  # Cambia esto al host de tu base de datos
portPg = "5432"       # Puerto predeterminado de PostgreSQL

try:
    ##*************************************** ORACLE SPI
    connection = cx_Oracle.connect(username, password, dsn)
    print("Conexión exitosa a Oracle SPI")

    cursor = connection.cursor()
   #conecta con la table de control de ingreso de empleados
    connectionPg = psycopg2.connect(
        dbname=dbnamePg,
        user=userPg,
        password=passwordPg,
        host=hostPg,
        port=portPg
    )
    print("Conexión exitosa a PostgreSQL")
    cursorApiEmpleado = connectionPg.cursor()
    try:
        # Iniciar la transacción
        connection.begin()
        ##########connectionPg.autocommit = False ####esto se coloca para probar. Pero es recomendable este en automatico para que registre el LOG
        sql_query = "SELECT * FROM empleados where ID  =22147"
        #sql_query = "SELECT * FROM empleados where employee_id ='3662' and event_type  in ('employee_update','job_movement') and status_process is null"
        cursorApiEmpleado.execute(sql_query)
        results = cursorApiEmpleado.fetchall()
        employee_id=''
        for row in results:
            employee_id=row[0]
            transacction_id=row[6]
            ci_id=row[8]
            company_id=row[9]
            print('empleado json',employee_id,'',transacction_id,ci_id,company_id)

            ##*************************************** API BUK
            api_url = "https://alfonzorivas.buk.co/api/v1/colombia/employees/"+employee_id
            headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
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
                print("El colaborador NO EXISTE")
                # L   O   G   ****************************************************************
                Actividad = "El colaborador NO EXISTE"
                Estatus = "INFO"
                fecha_actual = datetime.now()
                # consulta = "INSERT INTO public.log "+ \
                # "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                # "VALUES(%s, %s, %s, %s, %s, %s)"
                # cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
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
                Actividad = "El colaborador EXISTE"
                Estatus = "INFO"
                fecha_actual = datetime.now()
                # consulta = "INSERT INTO public.log "+ \
                # "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                # "VALUES(%s, %s, %s, %s, %s, %s)"
                # cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
            api_url = "https://alfonzorivas.buk.co/api/v1/colombia/companies"
            headers = {'auth_token': 'QfhEF5gmYtzU26M6eE8xB4BY'}
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
            Buk_SEDE=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Sede"))[:30]
            Actividad = "Se crea una relación laboral en TA_RELACION_LABORAL"
            Estatus = "INFO"
            fecha_actual = datetime.now()
            consulta = "Select  codloc from public.localidades where Sede=%s and CIA_CODCIA=%s  "
            cursorApiEmpleado.execute(consulta, (Buk_SEDE, ID_EMPRESA))
            results = cursorApiEmpleado.fetchone()
            Buk_LOCALIDAD=results[0]
            #print('Buk_LOCALIDAD',Buk_LOCALIDAD)
            Buk_NOMBRE=(dataEmpleado.get("data", []).get("first_name"))[:120]
            partes = Buk_NOMBRE.split(" ")
            if len(partes) >= 2:
                Buk_NOMBRE1 = partes[0].upper()
                Buk_NOMBRE2 = partes[1].upper()
            else:
                Buk_NOMBRE1=(dataEmpleado.get("data", []).get("first_name")).upper()[:120]
                Buk_NOMBRE2=""
            Buk_APELLIDO1=(dataEmpleado.get("data", []).get("surname")).upper()[:17]
            Buk_APELLIDO2=(dataEmpleado.get("data", []).get("second_surname")).upper()[:15]
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
            if dataEmpleado.get("data", []).get("personal_email"):
                Buk_E_MAIL1=(dataEmpleado.get("data", []).get("personal_email"))[:120]
            else:
                Buk_E_MAIL1=''
            Buk_E_MAIL2=''
            Buk_IN_REL_TRAB=''
            Buk_USRCRE='ETL'
            if count_result == 1:         
                if results_eo_persona[1] != Buk_NOMBRE1:
                    print('Diferencia en NOMBRE1= SPI:',results_eo_persona[1]," BUK:",Buk_NOMBRE1)
                if results_eo_persona[2] != Buk_NOMBRE2:
                    print('Diferencia en NOMBRE2= SPI:',results_eo_persona[2]," BUK:",Buk_NOMBRE2)
                if results_eo_persona[3] != Buk_APELLIDO1:
                    print('Diferencia en APELLIDO1= SPI:',results_eo_persona[3]," BUK:",Buk_APELLIDO1)
                if results_eo_persona[4] != Buk_APELLIDO2:
                    print('Diferencia en APELLIDO2= SPI:',results_eo_persona[4]," BUK:",Buk_APELLIDO2)
                if results_eo_persona[5] != Buk_ID_TIPO_IDEN:
                    print('Diferencia en ID_TIPO_IDEN= SPI:',results_eo_persona[5]," BUK:",Buk_ID_TIPO_IDEN,"!")
                if results_eo_persona[6] != Buk_NACIONAL:
                    print('Diferencia en NACIONAL= SPI:',results_eo_persona[6]," BUK:",Buk_NACIONAL)
                if results_eo_persona[7] != Buk_NUM_IDEN:
                    print('Diferencia en NUM_IDEN= SPI:',results_eo_persona[7]," BUK:",Buk_NUM_IDEN)
                if results_eo_persona[8] != Buk_PASAPORTE:
                    print('Diferencia en PASAPORTE= SPI:',results_eo_persona[8]," BUK:",Buk_PASAPORTE)
                if results_eo_persona[9].strftime('%Y-%m-%d') != Buk_FECHA_NA:
                    print('Diferencia en FECHA_NA= SPI:',results_eo_persona[9].strftime('%Y-%m-%d')," BUK:",Buk_FECHA_NA)
                if results_eo_persona[10] != Buk_CIUDAD_NA:
                    print('Diferencia en CIUDAD_NA= SPI:',results_eo_persona[10]," BUK:",Buk_CIUDAD_NA)
                if results_eo_persona[11] != Buk_ID_ENTFE_NA:
                    print('Diferencia en ID_ENTFE_NA= SPI:',results_eo_persona[11]," BUK:",Buk_ID_ENTFE_NA)
                if results_eo_persona[12] != Buk_ID_PAIS_NA:
                    print('Diferencia en ID_PAIS_NA= SPI:',results_eo_persona[12]," BUK:",Buk_ID_PAIS_NA)
                if results_eo_persona[13] != Buk_SEXO:
                    print('Diferencia en SEXO= SPI:',results_eo_persona[13]," BUK:",Buk_SEXO)
                if results_eo_persona[14] != Buk_EDO_CIVIL:
                    print('Diferencia en EDO_CIVIL= SPI:',results_eo_persona[14]," BUK:",Buk_EDO_CIVIL)
                if results_eo_persona[15] != Buk_ZURDO:
                    print('Diferencia en ZURDO= SPI:',results_eo_persona[15]," BUK:",Buk_ZURDO)
                if results_eo_persona[18] != Buk_DIRECCION:
                    print('Diferencia en DIRECCION= SPI:',results_eo_persona[18]," BUK:",Buk_DIRECCION)
                if results_eo_persona[19] != Buk_CIUDAD:
                    print('Diferencia en CIUDAD= SPI:',results_eo_persona[19]," BUK:",Buk_CIUDAD)
                if results_eo_persona[20] != Buk_ID_ENTFE:
                    print('Diferencia en ID_ENTFE= SPI:',results_eo_persona[20]," BUK:",Buk_ID_ENTFE)
                if results_eo_persona[21] != Buk_ID_PAIS:
                    print('Diferencia en ID_PAIS= SPI:',results_eo_persona[21]," BUK:",Buk_ID_PAIS)             
                if results_eo_persona[25] != Buk_TELEFONO1:
                    print('Diferencia en TELEFONO1= SPI:',results_eo_persona[25]," BUK:",Buk_TELEFONO1)
                if results_eo_persona[28] != Buk_CELULAR:
                    print('Diferencia en CELULAR= SPI:',results_eo_persona[28]," BUK:",Buk_CELULAR)
                if results_eo_persona[29] != Buk_E_MAIL1:
                    print('Diferencia en E_MAIL1= SPI:',results_eo_persona[29]," BUK:",Buk_E_MAIL1)
                if results_eo_persona[31] != Buk_IN_REL_TRAB:
                    print('Diferencia en IN_REL_TRAB= SPI:',results_eo_persona[31]," BUK:",Buk_IN_REL_TRAB)
       
                #BUSCAR INFORMACION EN TA_RELACION_LABORAL PARA CALIDAR SI HAY CAMBIOS EN LA FICHA DEL JEFE O EN LA LOCALIDAD O SI LA EMPRESA ES L AMISMA QUE EN BUK (CASO TRANSFERENCIA) 
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
                if Buk_FICHA!=FICHA_RelacionLaboral:
                    print('Diferencia en FICHA. SPI;',FICHA_RelacionLaboral,' BUK:',Buk_FICHA) 
                if company_id!=ID_EMPRESA_RelacionLaboral:
                    print('empresas diferentes CASO TRANSFERENCIA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                else:
                    print('Igual compañia BUK y SPI. Se procede a evaluar promoción.')
                    print('LOCAIDADES:',ID_LOCALIDAD_RelacionLaboral,Buk_LOCALIDAD)
                    if ID_LOCALIDAD_RelacionLaboral != Buk_LOCALIDAD:
                        # SE ENCONTRO LOCALIDAD DIFERENTE Y SE CAMBIA EN SPI
                        parametros = {
                         'id_empresa':company_id,
                         'Buk_FICHA':Buk_FICHA,
                         'LOCALIDAD':Buk_LOCALIDAD,
                        }
                        consulta = """
                            UPDATE TA_RELACION_LABORAL SET ID_LOCALIDAD=:LOCALIDAD
                             WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND F_RETIRO IS NULL
                         """
                        cursor.execute(consulta, parametros)
                         # L   O   G   ****************************************************************
                        Actividad = "Cambio de Localidad: SPI="+ID_LOCALIDAD_RelacionLaboral+", BUK="+Buk_LOCALIDAD
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
                    print('FICHA_JEFE:',FICHA_JEFE_RelacionLaboral,Buk_FICHA_JEFE)
                    if FICHA_JEFE_RelacionLaboral!=Buk_FICHA_JEFE:
                        if company_id==Buk_ID_EMPRESA_BOSS:
                            # SE ENCONTRO FICHA SE JEFE DIFEERENTE Y SE CAMBIA EN SPI SOLO SI LAS EMPRESAS DEL EMPLEADO Y JEFE SON LAS MISMAS
                            Buk_FICHA_JEFE=results_boss[1]
                            parametros = {
                            'id_empresa':company_id,
                            'Buk_FICHA':Buk_FICHA,
                            'JEFE':Buk_FICHA_JEFE,
                            }
                            consulta = """
                                UPDATE TA_RELACION_LABORAL SET FICHA_JEFE=:JEFE
                                WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND F_RETIRO IS NULL
                            """
                            cursor.execute(consulta, parametros)
                            # L   O   G   ****************************************************************
                            Actividad = "Cambio en Ficha de Coach: SPI="+FICHA_JEFE_RelacionLaboral+", BUK="+Buk_FICHA_JEFE
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
                            Actividad = "El jefe con la ficha: "+Buk_FICHA_JEFE+ ", existe en la empresa: "+Buk_ID_EMPRESA_BOSS+", diferente a la empresa: "+company_id+" del colaborador."
                            Estatus = "ERROR"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************    
                            Buk_FICHA_JEFE=' '         
                        print('boss',Buk_ID_EMPRESA_BOSS,Buk_FICHA_JEFE)
                        print('ojo',company_id,Buk_FICHA)
                    #endif 


                    #VARIABLES A UTILIZAR EN AS DIFERENTES VALIDACIONES
                    id_cambio=''
                    Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))
                    Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
                    Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))[:5]
                    Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado")).upper()
                    Buk_Nivel_de_Seniority=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Nivel_de_Seniority")).upper()
                    Buk_ID_CARGO_NOMBRE_SR= Buk_ID_CARGO_NOMBRE+" "+Buk_Nivel_de_Seniority
                    print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
                    print('Buk_ID_CARGO',Buk_ID_CARGO)
                    print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO_NOMBRE)
                    print('Buk_Nivel_de_Seniority',Buk_Nivel_de_Seniority)
                    print('Buk_ID_CARGO_NOMBRE_SR',Buk_ID_CARGO_NOMBRE_SR)

                    

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
                        print('NO EXISTE TA_RELACION_PUESTO SE DEBE CREAR')

                    
                    
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
                    print('results_EO_CARGO',results_EO_CARGO)
                    if results_EO_CARGO is  None or results_EO_CARGO[0] is  None or not results_EO_CARGO:
                        id_cambio='10017'  # PROMOCION
                        print('NO EXISTE EO_CARGO SE DEBE CREAR')
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
                        Actividad = "Se crea en EO_CARGO el cargo codigo:"+str(Buk_ID_CARGO)+' Nombre:'+Buk_ID_CARGO_NOMBRE
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************

                    # CARGAMOS LA INFORMACION DEL PUESTO ACTUAL PARA COMPRAR UN POSIBLE CAMBIO DE SENIORITY
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
                    
                    print(results_EO_PUESTO,parametros)
                    #NOMBRE_PUESTO_results_EO_PUESTO=results_EO_PUESTO[3]
                    if not results_EO_PUESTO:
                        print('NO EXISTE EO_PUESTO SE DEBE CREAR')
                        id_cambio='10017'
                        if Buk_ID_UNIDAD!=results_TA_RELACION_PUESTO[3]:
                           id_cambio='20010' # RECLASIFICACION
                           # L   O   G   ****************************************************************
                           Actividad = "Se identifica una reclasificacion de UNIDAD. SPI :"+results_TA_RELACION_PUESTO[3]+" y BUK :"+Buk_ID_UNIDAD+" "
                           Estatus = "INFO"
                           fecha_actual = datetime.now()
                           consulta = "INSERT INTO public.log "+ \
                           "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                           "VALUES(%s, %s, %s, %s, %s, %s)"
                           cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    else:
                        #####*********************************************
                        if results_EO_PUESTO[3]!=Buk_ID_CARGO_NOMBRE_SR:
                           print('Cambio de seniority')
                           id_cambio='10017'  # PROMOCION
                           print(results_EO_PUESTO[3],Buk_ID_CARGO_NOMBRE_SR)
                           print('Cambio de seniority')
                           # L   O   G   ****************************************************************
                           Actividad = "Se procesa un cambio de Senionity EO_PUESTO. SPI :"+results_EO_PUESTO[3]+" y BUK :"+Buk_ID_CARGO_NOMBRE_SR+" "
                           Estatus = "INFO"
                           fecha_actual = datetime.now()
                           consulta = "INSERT INTO public.log "+ \
                           "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                           "VALUES(%s, %s, %s, %s, %s, %s)"
                           cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                           #####*********************************************

                    if id_cambio!='':
                        if False:
                            # SE DA DE ALTA EL PUESTO CON EL SENIONITY ANTERIOR++++++++++++++++++++++++++++++++++++++++++++++++
                            values_eo_puesto = {   
                                'Buk_ID_EMPRESA' :company_id,
                                'Buk_ID_UNIDAD' :results_TA_RELACION_PUESTO[3],
                                'Buk_ID_PUESTO' :results_TA_RELACION_PUESTO[4],
                                'Buk_ID_CARGO' :Buk_ID_CARGO,
                                'Buk_F_INGRESO' :Buk_F_INGRESO,
                                'Buk_USRCRE' :Buk_USRCRE,
                            } 
                            sql_query = """
                                    UPDATE INFOCENT.EO_PUESTO SET 
                                    FECHA_FIN=TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD'),
                                    USRACT=:Buk_USRCRE, 
                                    FECACT=SYSDATE
                                    WHERE ID_EMPRESA = :Buk_ID_EMPRESA 
                                    AND ID = :Buk_ID_PUESTO 
                                    AND ID_UNIDAD = :Buk_ID_UNIDAD 
                                    AND ID_CARGO= :Buk_ID_CARGO 
                                    AND FECHA_FIN>SYSDATE
                            """ 
                            print('update',consulta,values_eo_puesto)
                            cursor.execute(sql_query, values_eo_puesto)
                            # L   O   G   ****************************************************************
                            Actividad = "Se coloca FECHA_FIN en EO_PUESTO. id :"+str(results_TA_RELACION_PUESTO[4])+" ( "+results_EO_PUESTO[3]+" )"
                            Estatus = "INFO"
                            fecha_actual = datetime.now()
                            consulta = "INSERT INTO public.log "+ \
                            "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                            "VALUES(%s, %s, %s, %s, %s, %s)"
                            cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                            #####*********************************************
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
                        print('Buk_ID_PUESTO',Buk_ID_PUESTO,parametros,resultados_nuevo_puesto)


                        values_eo_puesto = {   
                            'Buk_ID_EMPRESA' :company_id,
                            'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                            'Buk_ID_PUESTO':Buk_ID_PUESTO,
                            'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE_SR,
                            'Buk_ID_CARGO':Buk_ID_CARGO,
                            'Buk_F_INGRESO':Buk_F_INGRESO,
                            'Buk_USRCRE' :Buk_USRCRE,
                        }    
                        print('values_eo_puesto',values_eo_puesto)              

                        # SE CREA EL NUEVO SENIORITY
                        sql_query = "INSERT INTO INFOCENT.EO_PUESTO "+ \
                        "(ID_EMPRESA, ID_UNIDAD, ID, NOMBRE, ID_CARGO, FECHA_INI,FECHA_FIN,USRCRE, FECCRE) "+ \
                        "VALUES(:Buk_ID_EMPRESA, :Buk_ID_UNIDAD, :Buk_ID_PUESTO, :Buk_ID_CARGO_NOMBRE, :Buk_ID_CARGO,TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') ,SYSDATE+300000, :Buk_USRCRE, SYSDATE)"
                        
                        cursor.execute(sql_query,values_eo_puesto)
                        print('2')
                        # L   O   G   ****************************************************************
                        Actividad = "Se crea un NUEVO EO_PUESTO. id :"+str(Buk_ID_PUESTO)+" ( "+Buk_ID_CARGO_NOMBRE_SR+" )"
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                        #####*********************************************
                    
                        # SE DA FECHA DE EGRESO A LA RELACIUON PUESTO ANTERIOR
                        parametros = {
                            'id_empresa':company_id,
                            'Buk_FICHA':Buk_FICHA,
                            'Buk_F_INGRESO':Buk_F_INGRESO,
                        }
                        consulta = """
                                UPDATE TA_RELACION_PUESTO SET FECHA_FIN=TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD')- INTERVAL '1' DAY
                                WHERE ID_EMPRESA = :id_empresa AND FICHA = :Buk_FICHA AND FECHA_FIN IS NULL
                        """
                        cursor.execute(consulta, parametros)
                        print('3')

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
                        Actividad = "Se crea una TA_RELACION_PUESTO "
                        Estatus = "INFO"
                        fecha_actual = datetime.now()
                        consulta = "INSERT INTO public.log "+ \
                        "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                        "VALUES(%s, %s, %s, %s, %s, %s)"
                        cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                # #####********************************************

                if False:
                    a=''

                    # #####*********************************************

                    # # PREPARANDO DATA TA_RELACION_LABORAL 
                    # Buk_ID_EMPRESA=ID_EMPRESA
                    # Buk_ID_PERSONA=Buk_ID
                    # Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))
                    # Buk_F_CORPORACION=Buk_F_INGRESO
                    # Buk_F_AJUSTADA1=Buk_F_INGRESO
                    # Buk_F_AJUSTADA2=Buk_F_INGRESO
                    # Buk_F_AJUSTADA3=Buk_F_INGRESO
                    # Buk_ID_LOCALIDAD=Buk_LOCALIDAD
                    # Buk_ID_CATEGORIA1=''
                    # Buk_ID_CATEGORIA2=''
                    # Buk_ID_CATEGORIA3=''
                    # Buk_ID_SINDICATO=''
                    # Buk_ID_CENTRO_MED=''
                    # Buk_NRO_RIF= (dataEmpleado.get("data", []).get("custom_attributes", {}).get("RIF"))
                    # Buk_ID_FINIQUITO=''
                    # Buk_NRO_SSO=''
                    # #BUSCAR LA FICHA DEL JEFE la cedula viene de Buk_BOSS=(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut"))[:30]
                    # sql_query = "SELECT ID_EMPRESA empresa,ficha " + \
                    # "FROM TA_RELACION_LABORAL trl ,EO_PERSONA ep "+ \
                    # "WHERE trl.ID_PERSONA  = ep.ID " + \
                    # "AND ID_EMPRESA!='BA' AND F_RETIRO IS NULL AND ep.NUM_IDEN ="+(dataEmpleado.get("data", []).get("current_job", {}).get("boss", {}).get("rut")).replace('.', '')[:30]
                    # cursor.execute(sql_query)
                    # results_boss = cursor.fetchone()
                    # Buk_ID_EMPRESA_BOSS=results_boss[0]
                    # Buk_FICHA_JEFE=results_boss[1]
                    # if Buk_ID_EMPRESA==Buk_ID_EMPRESA_BOSS:
                    #     Buk_FICHA_JEFE=results_boss[1]
                    # else:
                    #     # L   O   G   ****************************************************************
                    #     Actividad = "El jefe con la ficha: "+Buk_FICHA_JEFE+ ", existe en la empresa: "+Buk_ID_EMPRESA_BOSS+", diferente a la empresa: "+Buk_ID_EMPRESA+" del colaborador."
                    #     Estatus = "ERROR"
                    #     fecha_actual = datetime.now()
                    #     consulta = "INSERT INTO public.log "+ \
                    #     "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    #     "VALUES(%s, %s, %s, %s, %s, %s)"
                    #     cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #     #####*********************************************    
                    #     Buk_FICHA_JEFE=' '         
                    # print('boss',Buk_ID_EMPRESA_BOSS,Buk_FICHA_JEFE)
                    # print('ojo',Buk_ID_EMPRESA,Buk_FICHA)

                    # values_ta_relacion_laboral = {   
                    #     'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    #     'Buk_FICHA' :Buk_FICHA,
                    #     'Buk_ID_PERSONA' :Buk_ID_PERSONA,
                    #     'Buk_F_INGRESO' :Buk_F_INGRESO,
                    #     'Buk_F_CORPORACION' :Buk_F_CORPORACION,
                    #     'Buk_F_AJUSTADA1' :Buk_F_AJUSTADA1,
                    #     'Buk_F_AJUSTADA2' :Buk_F_AJUSTADA2,
                    #     'Buk_F_AJUSTADA3' :Buk_F_AJUSTADA3,
                    #     'Buk_ID_LOCALIDAD' :Buk_ID_LOCALIDAD,
                    #     'Buk_NRO_RIF' :'V'+str(Buk_NRO_RIF),
                    #     'Buk_NRO_SSO' :Buk_NRO_SSO,
                    #     'Buk_USRCRE' :Buk_USRCRE,
                    #     'Buk_FICHA_JEFE' :Buk_FICHA_JEFE,
                    # }    
                    # sql_query = "INSERT INTO TA_RELACION_LABORAL "+ \
                    # "(ID_EMPRESA, FICHA, ID_PERSONA, F_INGRESO, F_CORPORACION, F_AJUSTADA1, "+ \
                    # "F_AJUSTADA2, F_AJUSTADA3, ID_LOCALIDAD, NRO_RIF, NRO_SSO,  USRCRE, FECCRE, FICHA_JEFE) "+ \
                    # "VALUES(:Buk_ID_EMPRESA, :Buk_FICHA, :Buk_ID_PERSONA, TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') , TO_DATE(:Buk_F_CORPORACION, 'YYYY-MM-DD') , TO_DATE(:Buk_F_AJUSTADA1, 'YYYY-MM-DD') , "+ \
                    # " TO_DATE(:Buk_F_AJUSTADA2, 'YYYY-MM-DD'), TO_DATE(:Buk_F_AJUSTADA3, 'YYYY-MM-DD'), :Buk_ID_LOCALIDAD, :Buk_NRO_RIF, :Buk_NRO_SSO, "+ \
                    # ":Buk_USRCRE,SYSDATE,:Buk_FICHA_JEFE) "
                    # cursor.execute(sql_query,values_ta_relacion_laboral)
                    # # L   O   G   ****************************************************************
                    # Actividad = "Se crea una relación laboral en TA_RELACION_LABORAL"
                    # Estatus = "INFO"
                    # fecha_actual = datetime.now()
                    # consulta = "INSERT INTO public.log "+ \
                    # "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    # "VALUES(%s, %s, %s, %s, %s, %s)"
                    # cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))

                    # #####*********************************************
                    # # PREPARANDO DATA TA_RELACION_PUESTO 



                    
                    # Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
                    # Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))[:5]
                    # Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado")).upper()
                    # # SE INTENTA REUTILIZAR UN ID DE CARGO QUE ESTE DISPONIBLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!     Buk_ID_EMPRESA
                    # parametros = {
                    #     'id_empresa':Buk_ID_EMPRESA,
                    #     'Buk_ID_CARGO':Buk_ID_CARGO,
                    # }
                    # consulta = """
                    #         SELECT * FROM EO_CARGO ep 
                    #         WHERE ID_EMPRESA = :id_empresa AND ID = :Buk_ID_CARGO
                    # """
                    # cursor.execute(consulta, parametros)
                    # resultados = cursor.fetchone()
                    # print('resultados CARGO',resultados)
                    # if resultados is  None or resultados[0] is  None:
                    #     print('no existe el cargo')
                    #     # CREAR EL NUEVO CARGO+++++++++++++++++++++++++++++++++++++++++++++++++
                    #     values_eo_cargo = {   
                    #         'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    #         'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE,
                    #         'Buk_ID_CARGO':Buk_ID_CARGO,
                    #         'Buk_USRCRE' :Buk_USRCRE,
                    #     }  
                    #     sql_query = "INSERT INTO INFOCENT.EO_CARGO "+ \
                    #     "(      ID_EMPRESA, ID,  NOMBRE,  USRCRE,  FECCRE) "+ \
                    #     "VALUES(:Buk_ID_EMPRESA,  :Buk_ID_CARGO ,:Buk_ID_CARGO_NOMBRE, :Buk_USRCRE, SYSDATE)"
                    #     cursor.execute(sql_query,values_eo_cargo)
                    #     # L   O   G   ****************************************************************
                    #     Actividad = "Se crea una EO_CARGO "
                    #     Estatus = "INFO"
                    #     fecha_actual = datetime.now()
                    #     consulta = "INSERT INTO public.log "+ \
                    #     "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    #     "VALUES(%s, %s, %s, %s, %s, %s)"
                    #     cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #     #####*********************************************
                    # # SE INTENTA REUTILIZAR UN ID DE CARGO QUE ESTE DISPONIBLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!     Buk_ID_EMPRESA
                    # parametros = {
                    #     'id_empresa':Buk_ID_EMPRESA,
                    #     'id_unidad':Buk_ID_UNIDAD,
                    #     'Buk_ID_CARGO':Buk_ID_CARGO,
                    # }
                    # consulta = """
                    #     SELECT min(id) FROM (
                    #         SELECT ID FROM EO_PUESTO ep 
                    #         WHERE ID_EMPRESA = :id_empresa AND ID_UNIDAD = :id_unidad and ID_CARGO = :Buk_ID_CARGO
                    #         MINUS
                    #         -- PUESTOS QUE ESTÁ SIENDO OCUPADOS PARA ESE CARGO
                    #         SELECT A.ID_PUESTO FROM TA_RELACION_PUESTO A, EO_PUESTO B
                    #         WHERE A.ID_EMPRESA = :id_empresa AND A.ID_UNIDAD = :id_unidad AND A.FECHA_FIN IS NULL
                    #         AND A.ID_EMPRESA = B.ID_EMPRESA AND A.ID_UNIDAD = B.ID_UNIDAD AND  B.ID_CARGO=:Buk_ID_CARGO
                    #         AND A.ID_PUESTO = B.ID 
                    #     )
                    # """
                    # cursor.execute(consulta, parametros)
                    # resultados = cursor.fetchone()
                    # print('resultados',resultados)
                    # Buk_Nivel_de_Seniority=(dataEmpleado.get("data", []).get("current_job", {}).get("custom_attributes", {}).get("Nivel_de_Seniority")).upper()
                    # if resultados is  None or resultados[0] is  None:
                    #     # DETERMINAR EL PROXIMO ID PARA EL PUESTO ++++++++++++++++++++++++++++++
                    #     parametros = {
                    #         'id_empresa':Buk_ID_EMPRESA,
                    #         'id_unidad':Buk_ID_UNIDAD,
                    #     }
                    #     print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
                    #     print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)

                    #     consulta = """
                    #         SELECT NVL(MAX(id), 0) + 1  FROM EO_PUESTO ep 
                    #         WHERE ID_EMPRESA =:id_empresa
                    #         AND ID_UNIDAD=:id_unidad
                    #     """
                    #     cursor.execute(consulta, parametros)
                    #     resultados_nuevo_puesto = cursor.fetchone()   
                    #     Buk_ID_PUESTO=resultados_nuevo_puesto[0]   
                    #     print('Buk_ID_PUESTO',Buk_ID_PUESTO,'Buk_ID_CARGO:',Buk_ID_CARGO,'Buk_ID_UNIDAD:',Buk_ID_UNIDAD)
                    #     # CREAR EL NUEVO PUESTO+++++++++++++++++++++++++++++++++++++++++++++++++
                    #     values_eo_puesto = {   
                    #         'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    #         'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                    #         'Buk_ID_PUESTO':Buk_ID_PUESTO,
                    #         'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE+" "+Buk_Nivel_de_Seniority,
                    #         'Buk_ID_CARGO':Buk_ID_CARGO,
                    #         'Buk_F_INGRESO':Buk_F_INGRESO,
                    #         'Buk_USRCRE' :Buk_USRCRE,
                    #     }  
                    #     sql_query = "INSERT INTO INFOCENT.EO_PUESTO "+ \
                    #     "(ID_EMPRESA, ID_UNIDAD, ID, NOMBRE, ID_CARGO, FECHA_INI, FECHA_FIN, USRCRE, FECCRE) "+ \
                    #     "VALUES(:Buk_ID_EMPRESA, :Buk_ID_UNIDAD, :Buk_ID_PUESTO, :Buk_ID_CARGO_NOMBRE, :Buk_ID_CARGO,TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD') , SYSDATE+300000, :Buk_USRCRE, SYSDATE)"
                    #     cursor.execute(sql_query,values_eo_puesto)
                    #     # L   O   G   ****************************************************************
                    #     Actividad = "Se crea un NUEVO EO_PUESTO. id :"+str(Buk_ID_PUESTO)+" ( "+Buk_ID_CARGO_NOMBRE+" )"
                    #     Estatus = "INFO"
                    #     fecha_actual = datetime.now()
                    #     consulta = "INSERT INTO public.log "+ \
                    #     "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    #     "VALUES(%s, %s, %s, %s, %s, %s)"
                    #     cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #     #####*********************************************
                    # else:
                    #     Buk_ID_PUESTO=resultados[0]
                    #     # L   O   G   ****************************************************************
                    #     Actividad = "Se encontro el Id  :"+str(Buk_ID_PUESTO)+" disponible en EO_PUESTO."
                    #     Estatus = "INFO"
                    #     fecha_actual = datetime.now()
                    #     consulta = "INSERT INTO public.log "+ \
                    #     "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    #     "VALUES(%s, %s, %s, %s, %s, %s)"
                    #     cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #     #####*********************************************
                    # print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO,Buk_ID_CARGO_NOMBRE)
                    # print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
                    # print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
                    # print('Buk_ID_PUESTO',Buk_ID_PUESTO)
                    # values_ta_relacion_puesto = {   
                    #     'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    #     'Buk_FICHA' :Buk_FICHA,
                    #     'Buk_F_INGRESO' :Buk_F_INGRESO,
                    #     'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                    #     'Buk_ID_PUESTO' :Buk_ID_PUESTO,
                    #     'Buk_ID_CAMBIO':'10001',
                    #     'Buk_USRCRE' :Buk_USRCRE, 
                    # }  
                    # sql_query = "INSERT INTO TA_RELACION_PUESTO "+ \
                    # "(ID_EMPRESA, FICHA, FECHA_INI, ID_UNIDAD, ID_PUESTO,ID_CAMBIO, USRCRE, FECCRE) "+ \
                    # "VALUES(:Buk_ID_EMPRESA, :Buk_FICHA, TO_DATE(:Buk_F_INGRESO, 'YYYY-MM-DD'), :Buk_ID_UNIDAD, :Buk_ID_PUESTO,:Buk_ID_CAMBIO, :Buk_USRCRE, SYSDATE) "
                    # #print(sql_query)
                    # cursor.execute(sql_query,values_ta_relacion_puesto)
                    # # L   O   G   ****************************************************************
                    # Actividad = "Se crea una TA_RELACION_PUESTO "
                    # Estatus = "INFO"
                    # fecha_actual = datetime.now()
                    # consulta = "INSERT INTO public.log "+ \
                    # "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    # "VALUES(%s, %s, %s, %s, %s, %s)"
                    # cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    # #####********************************************
            Estatus = "1"
            fecha_actual = datetime.now()
            consulta = "UPDATE empleados set status_process=%s,date_process=%s where id=%s "
            cursorApiEmpleado.execute(consulta, (Estatus, fecha_actual,transacction_id))
        

        #ENDFOR
        # Confirmar la transacción si no hubo errores
        
        connection.commit()
        #connection.rollback()
        
        # LISTA EL CONTENIDO DEL LOG
        sql_query = "SELECT * FROM log"
        cursorApiEmpleado.execute(sql_query)
        results_log = cursorApiEmpleado.fetchall()
        #for row in results_log:
        #    print(row)
        connectionPg.commit()
        #connectionPg.rollback()
        print("Transacción exitosa")
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
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
