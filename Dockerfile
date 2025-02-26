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
    && rm -rf /var/lib/apt/lists/*

# Download and install Oracle Instant Client
RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linux.x64-21.6.0.0.0dbru.zip && \
    unzip instantclient-basiclite-linux.x64-21.6.0.0.0dbru.zip && \
    mv instantclient_21_6 /usr/lib/oracle/21.6/client64 && \
    rm instantclient-basiclite-linux.x64-21.6.0.0.0dbru.zip && \
    echo /usr/lib/oracle/21.6/client64 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

# Copy the requirements file into the container
COPY requirements.txt .

# Print the contents of the requirements file (for debugging)
RUN cat requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Run the Python script
CMD ["python", "wf0_main.py"]