"""
Empresa:DvConsultores
Por:    Jorge Luis Cuauro Gonzalez
Fecha:  Febrero 2025
Descripción:
    Este programa ejecuta una serie de programas en secuencia para realizar la interfaz BUK - SPI.
    Los programas a ejecutar son:
        - api0_actualiza.py
        - api0_promocion.py
        - wf1_carga_empleados_buk.py
        - wf2_carga_workflow.py
        - wf3_envio_correo.py
        - wf4_ingreso.py
    Cada uno de estos programas realiza una tarea específica y se ejecutan en secuencia.
    Al finalizar la
    ejecución de todos los programas, se muestra el tiempo transcurrido en minutos.
"""

from datetime import datetime
import subprocess
import os

# Obtener la ruta del directorio actual
directorio_actual = os.path.dirname(os.path.abspath(__file__))

programas = [
    "api0_actualiza.py",
    "api0_promocion.py",
    "wf1_carga_empleados_buk.py",
    "wf2_carga_workflow.py",
    "wf3_envio_correo.py",
    "wf4_ingreso.py",
]

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
