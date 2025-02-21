#import subprocess

#programas = [
#    "api0_actualiza.py",
#    "api0_promocion.py",
#    "wf1_carga_empleados_buk.py",
#    "wf2_carga_workflow.py",
#    "wf3_envio_correo.py",
#    "wf4_ingreso.py",
#]

#for programa in programas:
#    print(f"Ejecutando {programa}...")
#    subprocess.run(programa)

#print("Todos los programas se han ejecutado correctamente.")


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

for programa in programas:
    # Construir la ruta completa al archivo
    ruta_completa = os.path.join(directorio_actual, programa)
    
    print(f"Ejecutando {ruta_completa}...")
    
    # Ejecutar el script usando el intérprete Python
    subprocess.run(["python3", ruta_completa])

print("Todos los programas se han ejecutado correctamente.")
