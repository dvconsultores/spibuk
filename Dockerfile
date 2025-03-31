# Use the official Python slim image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Copy the Oracle Instant Client from the project root into the container
COPY instantclient_21_11 /opt/oracle/instantclient_21_11

# Create the required symlink (critical for cx_Oracle)
RUN cd /opt/oracle/instantclient_21_11 && \
    ln -s libclntsh.so.21.1 libclntsh.so

# Set Oracle environment variables
ENV ORACLE_HOME=/opt/oracle/instantclient_21_11
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
ENV PATH=$ORACLE_HOME:$PATH

# Update the linker cache (ensures the system finds the Oracle libraries)
RUN echo "/opt/oracle/instantclient_21_11" > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Run the Python script
CMD ["python", "wf0_main.py"]