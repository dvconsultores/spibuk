FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    libaio1 \
    gcc \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy and unzip Oracle Instant Client zip
COPY ./instantclient_21_11.zip /tmp/

RUN unzip /tmp/instantclient_21_11.zip -d /opt/oracle && \
    mv /opt/oracle/instantclient_21_11 /opt/oracle/instantclient && \
    rm /tmp/instantclient_21_11.zip

# Set Oracle environment variables
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
ENV PATH=$ORACLE_HOME:$PATH

# Symlinks for compatibility with cx_Oracle
RUN ln -sfn $ORACLE_HOME $ORACLE_HOME/lib && \
    ln -sf $ORACLE_HOME/libclntsh.so.21.1 $ORACLE_HOME/libclntsh.so && \
    ln -sf $ORACLE_HOME/libocci.so.21.1 $ORACLE_HOME/libocci.so && \
    ln -sf $ORACLE_HOME/libnnz21.so $ORACLE_HOME/libnnz.so && \
    echo "$ORACLE_HOME" > /etc/ld.so.conf.d/oracle.conf && \
    ldconfig

# Verify Oracle client setup
RUN ls -l /opt/oracle/instantclient && ldconfig -p | grep clntsh

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make the startup script executable
COPY start-services.sh .
RUN chmod +x ./start-services.sh

# Verify cx_Oracle is importable
RUN python -c "import cx_Oracle; print(f'✅ cx_Oracle {cx_Oracle.__version__} loaded successfully')"

CMD ["bash"]
