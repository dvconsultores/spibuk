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
passwordPg = "5YvFu7XA76vgq4aW1IUcvDO6ZHYhT9EF"
hostPg = "64.225.104.69"  # Cambia esto al host de tu base de datos
portPg = "5432"       # Puerto predeterminado de PostgreSQL


try:


    ##*************************************** ORACLE SPI
    connection = cx_Oracle.connect(username, password, dsn)
    print("Conexión exitosa a Oracle SPI")
  
    cursor = connection.cursor()
    #sql_query = 'SELECT * FROM EO_PERSONA'
    #cursor.execute(sql_query)

    #results = cursor.fetchall()
 
    #for row in results:
    #    print(row)



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

        #sql_query = "SELECT * FROM empleados where event_type  ='job_movement' and status_process is null"
        sql_query = "SELECT * FROM empleados where employee_id ='3313' and event_type  ='job_movement' and status_process is null"
        cursorApiEmpleado.execute(sql_query)
        results = cursorApiEmpleado.fetchall()

        employee_id=''
        for row in results:
            employee_id=row[0]
            transacction_id=row[6]
            print('empleado json',employee_id,'',transacction_id)

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
                    print(f"El valor de 'codigo_empresa' es: {ID_EMPRESA,}")
                    Buk_ID_EMPRESA=ID_EMPRESA
                else:
                    print("No se encontró ningún elemento con ID igual a 10.")

            else:
                print("Error al realizar la solicitud GET a la API. :", responseEmpresa.status_code)

            Buk_FICHA_JSON=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Ficha"))[:30]
            Buk_NUM_IDEN=(dataEmpleado.get("data", []).get("rut")).replace('.', '')[:20]
            Buk_NOMBRE=(dataEmpleado.get("data", []).get("full_name"))
            Buk_USRCRE='ETL'
            print('Buk_NUM_IDEN',Buk_NUM_IDEN)
            #print('Buk_FICHA_JSON',Buk_FICHA_JSON)
            sql_query = 'SELECT COUNT(*) FROM EO_PERSONA WHERE NUM_IDEN = '+Buk_NUM_IDEN
            #print('sql_query',sql_query)
            cursor.execute(sql_query)
            count_result = cursor.fetchone()[0]

            # Verificar si el COLABORADOR EXISTE
            if count_result == 0:
                print("El colaborador NO EXISTE")
                # L   O   G   ****************************************************************
                Actividad = "El colaborador NO EXISTE"
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************
            else:
                print(f"El colaborador EXISTE: {count_result}")

                #####*********************************************

                sql_query = 'SELECT ID FROM EO_PERSONA WHERE NUM_IDEN = '+Buk_NUM_IDEN
                #print('sql_query',sql_query)
                cursor.execute(sql_query)
                results_correlativo = cursor.fetchone()
                Buk_ID=results_correlativo[0]
                Buk_FICHA=Buk_FICHA_JSON

                # L   O   G   ****************************************************************
                Actividad = "El colaborador EXISTE"
                Estatus = "INFO"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))

                #IMPORTANTE. VALIDAR LA EMPRESA: SI ES LA MISMA QUE TIENE TA_RELACION_LABORAL CON JASON DE BUK ENTONCES ES UNA PROMISION DE LO CONTRARIO ES UNA TRASFERENCIA
                sql_query = "SELECT ID_EMPRESA empresa,ficha " + \
                "FROM TA_RELACION_LABORAL trl ,EO_PERSONA ep "+ \
                "WHERE trl.ID_PERSONA  = ep.ID " + \
                "AND ID_EMPRESA!='BA' AND F_RETIRO IS NULL AND ep.NUM_IDEN ="+Buk_NUM_IDEN
                cursor.execute(sql_query)
                results_SPI = cursor.fetchone()
                Buk_ID_EMPRESA_SPI=results_SPI[0]
                Buk_FICHA_SPI=results_SPI[1]
                if Buk_ID_EMPRESA==Buk_ID_EMPRESA_SPI:

                    # PREPARANDO DATA TA_RELACION_PUESTO 
                    Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
                    Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))
                    Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado"))
                    print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
                    print('Buk_ID_CARGO',Buk_ID_CARGO)
                    print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO_NOMBRE)





                    # L   O   G   ****************************************************************
                    Actividad = "Colaborador "+Buk_NOMBRE+"  en promoción."
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####***************




                else:
                    # L   O   G   ****************************************************************
                    Actividad = "Colaborador "+Buk_NOMBRE+", promovido de la empresa: "+Buk_ID_EMPRESA+", a la empresa : "+Buk_ID_EMPRESA_SPI
                    Estatus = "INFO"
                    fecha_actual = datetime.now()
                    consulta = "INSERT INTO public.log "+ \
                    "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                    "VALUES(%s, %s, %s, %s, %s, %s)"
                    cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                    #####***************               


                sql_query = "SELECT * FROM correlativos"
                cursorApiEmpleado.execute(sql_query)
                results_correlativo = cursorApiEmpleado.fetchone()
                Buk_FICHA=results_correlativo[0]
                print(Buk_ID)
                print(Buk_FICHA)


            #DETERMINAR EL CODIGO DE LOCALIDAD
            Buk_SEDE=(dataEmpleado.get("data", []).get("custom_attributes", {}).get("Sede"))[:30]

            consulta = "Select  codloc from public.localidades where Sede=%s and CIA_CODCIA=%s  "
            cursorApiEmpleado.execute(consulta, (Buk_SEDE, ID_EMPRESA))
            results = cursorApiEmpleado.fetchone()
            
            Buk_LOCALIDAD=results[0]
            print('Buk_LOCALIDAD',Buk_LOCALIDAD)

            
            # PREPARANDO DATA TA_RELACION_LABORAL 
            Buk_ID_EMPRESA=ID_EMPRESA
            Buk_ID_PERSONA=Buk_ID
            #Buk_F_INGRESO = datetime.strptime(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"), '%Y-%m-%d').strftime('%Y-%m-%d')
            Buk_F_INGRESO=(dataEmpleado.get("data", []).get("current_job", {}).get("start_date"))

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
            Buk_ID_EMPRESA_BOSS=results_boss[0]
            Buk_FICHA_JEFE=results_boss[1]
            if Buk_ID_EMPRESA==Buk_ID_EMPRESA_BOSS:
                Buk_FICHA_JEFE=results_boss[1]
            else:
                # L   O   G   ****************************************************************
                Actividad = "El jefe con la ficha: "+Buk_FICHA_JEFE+ ", existe en la empresa: "+Buk_ID_EMPRESA_BOSS+", diferente a la empresa: "+Buk_ID_EMPRESA+" del colaborador."
                Estatus = "ERROR"
                fecha_actual = datetime.now()
                consulta = "INSERT INTO public.log "+ \
                "(id_buk, fecha_proceso, id_spi, ficha_spi, actividad, status) "+ \
                "VALUES(%s, %s, %s, %s, %s, %s)"
                cursorApiEmpleado.execute(consulta, (transacction_id, fecha_actual,Buk_ID,Buk_FICHA,Actividad,Estatus))
                #####*********************************************    
                Buk_FICHA_JEFE=''            

            print('boss',Buk_ID_EMPRESA_BOSS,Buk_FICHA_JEFE)
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
                'Buk_NRO_RIF' :Buk_NRO_RIF,
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
            #####*********************************************

            # PREPARANDO DATA TA_RELACION_PUESTO 
            Buk_ID_UNIDAD=dataEmpleado.get("data", []).get("current_job", {}).get("cost_center")  #LA UNIDAD ES EL CENTRO DE COSTOS
            Buk_ID_CARGO=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("codigo_cargo"))
            Buk_ID_CARGO_NOMBRE=(dataEmpleado.get("data", []).get("current_job", {}).get("role", {}).get("custom_attributes", {}).get("cargo_abreviado"))


            # SE INTENTA REUTILIZAR UN ID DE CARGO QUE ESTE DISPONIBLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!     Buk_ID_EMPRESA
            parametros = {
                'id_empresa':Buk_ID_EMPRESA,
                'id_unidad':Buk_ID_UNIDAD,
                'Buk_ID_CARGO':Buk_ID_CARGO,
            }
            #print('Buk_ID_CARGO',Buk_ID_CARGO)
            #print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
            #print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)

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
            print('resultados',resultados)


            if resultados is  None or resultados[0] is  None:

                # DETERMINAR EL PROXIMO ID PARA EL PUESTO ++++++++++++++++++++++++++++++
                parametros = {
                    'id_empresa':Buk_ID_EMPRESA,
                    'id_unidad':Buk_ID_UNIDAD,
                }
                print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
                print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)

                consulta = """
                    SELECT NVL(MAX(id), 0) + 1  FROM EO_PUESTO ep 
                    WHERE ID_EMPRESA =:id_empresa
                    AND ID_UNIDAD=:id_unidad
                """
                cursor.execute(consulta, parametros)
                resultados_nuevo_puesto = cursor.fetchone()   
                Buk_ID_PUESTO=resultados_nuevo_puesto[0]   

                print('Buk_ID_PUESTO',Buk_ID_PUESTO)
                # CREAR EL NUEVO PUESTO+++++++++++++++++++++++++++++++++++++++++++++++++
                
                values_eo_puesto = {   
                    'Buk_ID_EMPRESA' :Buk_ID_EMPRESA,
                    'Buk_ID_UNIDAD' :Buk_ID_UNIDAD,
                    'Buk_ID_PUESTO':Buk_ID_PUESTO,
                    'Buk_ID_CARGO_NOMBRE' :Buk_ID_CARGO_NOMBRE,
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
            print('Buk_ID_CARGO_NOMBRE',Buk_ID_CARGO,Buk_ID_CARGO_NOMBRE)
            print('Buk_ID_UNIDAD',Buk_ID_UNIDAD)
            print('Buk_ID_EMPRESA',Buk_ID_EMPRESA)
            print('Buk_ID_PUESTO',Buk_ID_PUESTO)
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
            #####*********************************************


            Estatus = "1"
            fecha_actual = datetime.now()
            consulta = "UPDATE empleados set status_process=%s,date_process=%s where id=%s "
            cursorApiEmpleado.execute(consulta, (Estatus, fecha_actual,transacction_id))

            if count_result == 0:
                valor_actual = Buk_FICHA
                numero_actual = int(valor_actual[2:])
                nuevo_numero = numero_actual + 1
                ficha = f"BK{nuevo_numero}"

                id_empleado = int(Buk_ID)+1
                consulta = "UPDATE correlativos set ficha=%s,id_empleado=%s "
                cursorApiEmpleado.execute(consulta, (ficha, id_empleado))         

        #ENDFOR
        # Confirmar la transacción si no hubo errores
        #connection.commit()
        connection.rollback()

        # LISTA EL CONTENIDO DEL LOG
        sql_query = "SELECT * FROM log"
        cursorApiEmpleado.execute(sql_query)
        results_log = cursorApiEmpleado.fetchall()
        for row in results_log:
            print(row)


        #connectionPg.commit()
        connectionPg.rollback()

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




