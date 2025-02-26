# Use the official Python image from the Docker Hub
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies and Oracle Instant Client
RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    libaio1 \
    wget \
    unzip \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client (replace this if using a local file)
COPY instantclient-basiclite-linux.x64-21.6.0.0.0dbru.zip /tmp/
RUN unzip /tmp/instantclient-basiclite-linux.x64-21.6.0.0.0dbru.zip && \
    mv instantclient_21_6 /usr/lib/oracle/21.6/client64 && \
    rm /tmp/instantclient-basiclite-linux.x64-21.6.0.0.0dbru.zip && \
    echo /usr/lib/oracle/21.6/client64 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

# Set environment variables for cx_Oracle
ENV LD_LIBRARY_PATH="/usr/lib/oracle/21.6/client64:$LD_LIBRARY_PATH"
ENV ORACLE_HOME="/usr/lib/oracle/21.6/client64"

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Run the Python script
CMD ["python", "wf0_main.py"]
