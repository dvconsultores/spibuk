from flask import Flask, request, jsonify
import psycopg2

from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()
import os

postgre_user = os.getenv("POSTGRE_USER")
postgre_pass = os.getenv("POSTGRE_PASS")
postgre_host = os.getenv("POSTGRE_HOST")
postgre_port = os.getenv("POSTGRE_PORT")
postgre_service = os.getenv("POSTGRE_SERVICE")



app = Flask(__name__)

@app.route('/api/insertar', methods=['POST'])
def insertar_registro():
    try:
        data = request.get_json()
        employee_id = data['data']['employee_id']
        date = data['data']['date']
        event_type = data['data']['event_type']
        tenant_url = data['data']['tenant_url']

        connection = psycopg2.connect(
            host=postgre_host,
            port=postgre_port,
            database=postgre_service,
            user=postgre_user,
            password=postgre_pass
        )

        cursor = connection.cursor()
        #cursor.execute("INSERT INTO empleados (employee_id, date,event_type,tenant_url) VALUES (%s, %s, %s, %s)", (data['employee_id'], data['date'], data['event_type'], data['tenant_url']))
        cursor.execute("""
            INSERT INTO empleados (employee_id, date, event_type, tenant_url)
            VALUES (%s, %s, %s, %s)
        """, (employee_id, date, event_type, tenant_url))
        connection.commit()
        cursor.close()
        return jsonify({'mensaje': 'Registro insertado correctamente'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

if __name__ == '__main__':
    app.run(debug=True)