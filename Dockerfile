FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libaio1 \
    unzip \
    wget \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client (alternative to COPY if you want automatic download)
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2111000/instantclient-basiclite-linux.x64-21.11.0.0.0.zip \
    && unzip instantclient-basiclite-linux.x64-21.11.0.0.0.zip -d /opt/oracle \
    && mv /opt/oracle/instantclient_21_11 /opt/oracle/instantclient \
    && rm instantclient-basiclite-linux.x64-21.11.0.0.0.zip

# OR use your local copy (comment out the above RUN command if using this)
# COPY ./instantclient_21_11 /opt/oracle/instantclient

# Set Oracle environment variables
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
ENV PATH=$ORACLE_HOME:$PATH
ENV TZ=UTC

# Configure Oracle libraries
RUN cd $ORACLE_HOME \
    && ln -s libclntsh.so.21.1 libclntsh.so \
    && ln -s libociei.so liboci.so \
    && echo "$ORACLE_HOME" > /etc/ld.so.conf.d/oracle.conf \
    && ldconfig

# Verify Oracle installation
RUN ls -la $ORACLE_HOME && \
    ldd $ORACLE_HOME/libclntsh.so && \
    ldconfig -p | grep oracle

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Verify cx_Oracle installation
RUN python -c "import cx_Oracle; print(f'cx_Oracle {cx_Oracle.__version__} loaded successfully'); print('Client version:', cx_Oracle.clientversion())"

# Copy application code
COPY . .

# Default command (remove bash for production)
CMD ["bash"]