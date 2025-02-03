import subprocess

programas = [
    "wf1_carga_empleados_buk.py",
    "wf2_carga_workflow.py",
    "wf3_envio_correo.py",
    "wf4_ingreso.py",
]

for programa in programas:
    subprocess.run(programa)

print("Todos los programas se han ejecutado correctamente.")
