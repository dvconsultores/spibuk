#!/bin/bash

# Connect to FortiClient VPN
echo "Connecting to FortiClient VPN..."
forticlient vpn connect --host 200.74.233.50:10443 --username dvconsult --password 'Aros$**'

# Check if the VPN connection was successful
if [ $? -ne 0 ]; then
    echo "Failed to connect to FortiClient VPN. Exiting."
    exit 1
fi

echo "VPN connected successfully."

# Run the Python application
echo "Starting Python application..."
python3 /app/wf0_main.py