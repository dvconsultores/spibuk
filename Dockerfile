FROM python:3.11-slim

WORKDIR /app

# Install dependencies, including supervisor
RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    libaio1t64 \
    gcc \
    unzip \
    supervisor \
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

# Copy supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Start supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

name: Build and Push Docker Image

on:
  push:
    branches:
      - master

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v2

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v1

      - name: Log in to Docker Hub
        uses: docker/login-action@v2
        with:
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}

      - name: Build and push Docker image
        uses: docker/build-push-action@v2
        with:
          context: ./spibuk  # Set context to spibuk/ directory
          dockerfile: Dockerfile  # Dockerfile is now in the context root
          push: true
          tags: ${{ secrets.DOCKER_USERNAME }}/micro-buk-ar:latest
