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
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy the Oracle Instant Client files into the container
COPY instantclient_21_11 /usr/lib/oracle/21.11/client64

# Configure the Oracle Instant Client
ENV LD_LIBRARY_PATH=/usr/lib/oracle/21.11/client64:$LD_LIBRARY_PATH
ENV PATH=/usr/lib/oracle/21.11/client64:$PATH

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Run the Python script
CMD ["python", "wf0_main.py"]
