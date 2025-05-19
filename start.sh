#!/bin/bash

# Connect to VPN
echo "Connecting to VPN..."
forticlient vpn connect --host 200.74.233.50:10443 --username dvconsult --password 'Aros$**'

# Verify VPN connection
echo "Verifying VPN connection..."
if ping -c 3 200.74.233.50 > /dev/null 2>&1; then
    echo "✅ VPN connected successfully."
else
    echo "❌ VPN connection failed. Exiting."
    exit 1
fi

# Start the application
echo "Starting the application..."
python wf0_main.py