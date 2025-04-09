FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    libaio1 \
    gcc \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy Oracle Instant Client libs
COPY ./instantclient_21_11 /opt/oracle/instantclient

# Set Oracle env vars
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
ENV PATH=$ORACLE_HOME:$PATH

# Update linker config
RUN echo "$ORACLE_HOME" > /etc/ld.so.conf.d/oracle.conf && \
    ldconfig

# Force symlink for libclntsh.so
RUN ln -s $ORACLE_HOME/libclntsh.so.21.1 $ORACLE_HOME/libclntsh.so

# Verify libs
RUN ls -l $ORACLE_HOME && ldconfig -p | grep libclntsh

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Verify cx_Oracle
RUN python -c "import cx_Oracle; print(f'✅ cx_Oracle {cx_Oracle.__version__} loaded successfully')"

# CMD ["python", "wf0_main.py"]
CMD ["bash"]