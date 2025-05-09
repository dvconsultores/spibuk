#!/bin/bash
# Connect to VPN
forticlient vpn connect --host 200.74.233.50:10443 --username dvconsult --password 'Aros$**'

# Start the application
python wf0_main.py