# Stage 1: Download FortiClient VPN
FROM debian:bullseye-slim as downloader

WORKDIR /tmp

# Install wget and download the FortiClient VPN .deb file
RUN apt-get update && apt-get install -y wget && \
    wget https://filestore.fortinet.com/forticlient/downloads/forticlient_vpn_7.4.0.1636_amd64.deb

# Stage 2: Build the final image
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

# Copy FortiClient VPN from the downloader stage
COPY --from=downloader /tmp/forticlient_vpn_7.4.0.1636_amd64.deb /tmp/

# Install FortiClient VPN using dpkg and fix dependencies
RUN apt-get update && \
    apt-get install -y /tmp/forticlient_vpn_7.4.0.1636_amd64.deb && \
    apt-get -f install -y && \
    rm /tmp/forticlient_vpn_7.4.0.1636_amd64.deb

# Download and unzip Oracle Instant Client
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

# Add a script to manage VPN and application startup
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Set the entrypoint to the script
ENTRYPOINT ["/app/start.sh"]