"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
 Este programa gestiona el envio de correo para notificar la ficha reservada y posteriormente habiitar el INGRESO del colaborador.
 
 Esto se realiza mediante la informacion de la tala workflow_alta de PostgreSQL.

 Primer momento:
    Buscamos en workflow_api_email ese colaborador y verificamos si el colaborador existe en la tabla workflow_alta, si NO existe en workflow_alta se le reserva una una ficha, se 
    inserta en workflow_alta y se envia un correo.( Si existe, no se hace nada)

    INSERT INTO public.workflow_alta (id,document_number,ficha,fecha_email)

Segundo momento:
    Buscamos en workflow_ficha ese colaborador y verificamos si el colaborador existe en la tabla workflow_alta, si existe en workflow_alta actualizamos la tabla workflow_alta.
 
    update public.workflow_alta set employee_id=%s,name=%s,status_ingreso=%s WHERE id::integer = %s

Caso de inconsistencia:
    Si no se encuentra el colaborador (cédula) en la tabla empleados_buk, se envia un correo notificando la inconsistencia y se actualiza el status del workflow_alta en 2 para indivar que hay un error .


"""

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


postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")

#email_user = os.getenv("EMAIL_USER")
email_user = "interfazbukspi@alfonzorivas.com"
email_pass = os.getenv("EMAIL_PASS")


try:
    #  Configura la conexión al servidor SMTP
    server = smtplib.SMTP('smtp-relay.gmail.com', 587)

    #server = smtplib.SMTP('smtp.gmail.com', 587)
    #server.starttls()
    #server.login(email_user, email_pass)
    print("Conexión exitosa a smtp")

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
        ##########connectionPg.autocommit = False ####esto se coloca para probar. Pero es recomendable este en automatico para que registre el LOG
        sql_query = "select * from public.workflow_api_email  where status in ('En progreso','Aprobado') and document_number is not null"
        #sql_query = "select * from public.workflow_api_email where id in (1304)"
        cursorApiEmpleado.execute(sql_query)
        results = cursorApiEmpleado.fetchall()
        employee_id=''
        for row in results:

            transacction_id=row[0] 
            var_title=row[1] 
            var_kind=row[2] 
            var_document_number=row[3] 
            var_rut=row[4] 
            var_first_name=row[5] 
            var_last_name=row[6] 
            var_segundo_apellido=row[7] 
            var_start_date=row[8] 
            var_operations=row[9] 
            var_completed_at=row[10] 
            var_completed_by_document_number=row[11] 
            var_completed_by_document_type=row[12] 
            var_completed_by_rut=row[13] 
            var_completed_by_first_name=row[14] 
            var_completed_by_last_name=row[15] 
            var_completed_by_segundo_apellido=row[16] 
            var_completed_by_email=row[17] 
            var_status=row[18] 
            var_created_at=row[19] 
            var_created_by_document_number=row[20] 
            var_created_by_document_type=row[21] 
            var_created_by_rut=row[22] 
            var_created_by_first_name=row[23] 
            var_created_by_last_name=row[24] 
            var_created_by_segundo_apellido=row[25] 
            var_created_by_email=row[26] 
            #print('empleado ','',transacction_id)

            sql_query = 'SELECT COUNT(*) FROM public.workflow_alta WHERE id::integer = '+str(transacction_id)
            cursorApiEmpleado.execute(sql_query)
            count_workflow_alta= cursorApiEmpleado.fetchone()[0]
            #print(sql_query)
            # Verificar si el COLABORADOR EXISTE
            if count_workflow_alta == 0:
                #print("El colaborador NO EXISTE")
                # reservo el numero de ficha
                sql_query = "SELECT * FROM correlativos"
                cursorApiEmpleado.execute(sql_query)
                results_correlativo = cursorApiEmpleado.fetchone()
                Buk_FICHA=results_correlativo[0]
                #print(Buk_FICHA)
                # Crea el mensaje
                msg = MIMEMultipart()
                msg['Subject'] = 'Asignación de Ficha Nro: ' + Buk_FICHA + '. Colaborador: ' + var_first_name + ', ' + var_last_name
                msg['From'] = email_user
                msg['To'] = 'jhidalgo@alfonzorivas.com'
                # Crea el cuerpo del mensaje
                cuerpo_mensaje = f"""Estimado/a ,

Le informamos que se ha asignado el número de ficha {Buk_FICHA} para {var_first_name} {var_last_name}, nro de Documento: {var_document_number}

**Detalles del workflow:**

Id del workflow:    {transacction_id}



Atentamente,

Sistema Automático de gestión de ingresos.
                """
                msg.attach(MIMEText(cuerpo_mensaje, 'plain'))
                 # Envía el correo electrónico
                server.sendmail(email_user, 'jhidalgo@alfonzorivas.com', msg.as_string())

                fecha_actual = datetime.now()

                # creo el colaborador en tabla de control
                sql_query = "INSERT INTO public.workflow_alta (id,document_number,ficha,fecha_email) values (%s,%s,%s,%s)"
                cursorApiEmpleado.execute(sql_query,(transacction_id,var_document_number,Buk_FICHA,fecha_actual))
                
                # incremento el numero de ficha
                valor_actual = Buk_FICHA
                numero_actual = int(valor_actual[2:])
                nuevo_numero = numero_actual + 1
                fichas = f"BK{nuevo_numero}"
                sql_query = f"update public.correlativos set ficha = '{fichas}'"
                cursorApiEmpleado.execute(sql_query)

            else:
                sql_query = 'SELECT status_ingreso,ficha FROM public.workflow_alta WHERE id::integer = '+str(transacction_id)
                cursorApiEmpleado.execute(sql_query)
                resultado_workflow_alta = cursorApiEmpleado.fetchone()
                validaficha_workflow_alta= resultado_workflow_alta[0]
                Buk_FICHA=resultado_workflow_alta[1]
                if validaficha_workflow_alta is None: # valida que no se procesara aneriormente
                    sql_query = 'SELECT COUNT(*) FROM public.workflow_api_ficha WHERE id::integer = '+str(transacction_id)
                    cursorApiEmpleado.execute(sql_query)
                    count_result = cursorApiEmpleado.fetchone()[0]
                    # Verificar si el COLABORADOR EXISTE
                    if count_result != 0:
                        var_document_number = var_document_number.replace('.', '')  # Remove all periods
                        var_document_number = str(var_document_number)  # Convert to integer
                        #print('lll',var_document_number)

                        sql_query = "SELECT * FROM public.empleados_buk where CAST(replace(document_number, '.', '') AS INTEGER) = "+var_document_number
                        #print(sql_query)
                        cursorApiEmpleado.execute(sql_query)
                        count_result = cursorApiEmpleado.fetchone()
                        #print(count_result)
                        if count_result is None:
                            print(f"No se encontró empleado con documento: {var_document_number}")
                            #continue
                            msg = MIMEMultipart()
                            msg['Subject'] = 'Inconsistencia al preparar ingreso. Documento: ' + var_document_number + '. Colaborador: ' + var_first_name + ', ' + var_last_name
                            msg['From'] = email_user
                            msg['To'] = 'jhidalgo@alfonzorivas.com'
                            # Crea el cuerpo del mensaje
                            cuerpo_mensaje = f"""Estimado/a ,

Le informamos que en un proceso anterior, se notificó que fue asignado el número de ficha {Buk_FICHA} para {var_first_name} {var_last_name}, nro de Documento: {var_document_number} ,
Pero se encontró inconsistencia al preparar el ingreso, ya que no se encontró este colaborador en la tabla empleados_buk. 
El documento {var_document_number} no existe en el maestro de colaboradores de BUK.
El ingreso no se realizará hasta que se resuelva esta inconsistencia.

**Detalles del workflow:**

Id del workflow:    {transacction_id}



Atentamente,

Sistema Automático de gestión de ingresos.
                """
                            msg.attach(MIMEText(cuerpo_mensaje, 'plain'))
                            # Envía el correo electrónico
                            server.sendmail(email_user, 'jhidalgo@alfonzorivas.com', msg.as_string())
                            Estatus = "2" # Error al procesar el ingreso
                            consulta = "UPDATE public.workflow_alta set status_process=%s WHERE id::integer = %s"
                            cursorApiEmpleado.execute(consulta, (Estatus,transacction_id))
                            #para corregir el error de que no se encuentra el empleado en la tabla empleados_buk
                            #se debe actualizar el status de workflow_api_email en nulo y antes coregir el numero de cedula
                            #sql_query = "update public.workflow_api_email set status=null WHERE id::integer = %s"
                        else:
                            var_employee_id=count_result[1]
                            var_name=count_result[5]
                            fecha_actual = datetime.now()
                            sql_query = "update public.workflow_alta set employee_id=%s,name=%s,status_ingreso=%s WHERE id::integer = %s"
                            cursorApiEmpleado.execute(sql_query,(var_employee_id,var_name,fecha_actual,transacction_id))

        #ENDFOR
        # Confirmar la transacción si no hubo errores
        #connection.rollback()
        connectionPg.commit()
        #connectionPg.rollback()
        print("Transacción finalizada")
    except Exception as error:
        print(f"Ocurrió un error: {error}")
        # Manejar excepciones relacionadas con la base de datos
        # Realizar rollback en caso de error
        #connection.rollback()
    # Cierra el cursor y la conexión
    ########connectionPg.autocommit = True
    cursorApiEmpleado.close()
    connectionPg.close()
    server.quit()
except psycopg2.Error as e:
    print(f"Error al conectar a PostgreSQL: {e}")
