# Use the official Python slim image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    libaio1 \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy the Oracle Instant Client (ensure your local folder has correct structure)
COPY instantclient_21_11 /opt/oracle/instantclient_21_11

# Verify the copy worked
RUN ls -la /opt/oracle/instantclient_21_11/

# Create required symlinks
RUN cd /opt/oracle/instantclient_21_11 && \
    ln -sf libclntsh.so.21.1 libclntsh.so && \
    ln -sf libocci.so.21.1 libocci.so

# Set Oracle environment variables
ENV ORACLE_HOME=/opt/oracle/instantclient_21_11
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
ENV PATH=$ORACLE_HOME:$PATH

# Update linker configuration
RUN echo "/opt/oracle/instantclient_21_11" > /etc/ld.so.conf.d/oracle.conf && \
    ldconfig && \
    ldconfig -p | grep clntsh  # Verify the library is found

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add this right after the COPY command
RUN chmod -R +r /opt/oracle/instantclient_21_11 && \
    find /opt/oracle/instantclient_21_11 -type f -exec chmod a+r {} \; && \
    find /opt/oracle/instantclient_21_11 -type d -exec chmod a+rx {} \;

# Verify cx_Oracle can find the client
RUN python -c "import cx_Oracle; print(f'cx_Oracle version: {cx_Oracle.__version__}')"

# Copy application code
COPY . .

CMD ["python", "wf0_main.py"]