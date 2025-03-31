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

# Copy and reorganize Oracle Instant Client
COPY instantclient_21_11 /opt/oracle/instantclient_21_11

# Create lib directory and move libraries there (critical fix)
RUN mkdir -p /opt/oracle/instantclient_21_11/lib && \
    mv /opt/oracle/instantclient_21_11/*.so* /opt/oracle/instantclient_21_11/lib/

# Create required symlinks in lib directory
RUN cd /opt/oracle/instantclient_21_11/lib && \
    ln -sf libclntsh.so.21.1 libclntsh.so && \
    ln -sf libocci.so.21.1 libocci.so

# Set Oracle environment variables (point to lib directory)
ENV ORACLE_HOME=/opt/oracle/instantclient_21_11
ENV LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH

# Update linker configuration
RUN echo "/opt/oracle/instantclient_21_11/lib" > /etc/ld.so.conf.d/oracle.conf && \
    ldconfig

# Verify library detection
RUN ldconfig -p | grep clntsh && \
    ls -la /opt/oracle/instantclient_21_11/lib/

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final verification
RUN python -c "import cx_Oracle; print(f'cx_Oracle {cx_Oracle.__version__} loaded successfully')"

# Copy application code
COPY . .

CMD ["python", "wf0_main.py"]