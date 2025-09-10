#!/bin/sh

# Start the main worker script in the background
echo "Starting wf0_main.py..."
python wf0_main.py &

# Start the Flask app with Gunicorn for production
echo "Starting Flask app with Gunicorn on port 5000..."
# The format is [app_module_name]:[flask_instance_name]
# Assuming your file is app.py and the Flask instance is named app
gunicorn --workers 2 --bind 0.0.0.0:5000 app:app