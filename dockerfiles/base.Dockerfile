FROM python:3.7-slim-bullseye

# Install common packages
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    openjdk-17-jdk \
    bash \
    wget \
    pkg-config \
    libhdf5-dev \
    nano \
    vim \
    screen \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip setuptools wheel

# Common Python dependencies from requirements.txt
COPY requirements.txt /install/requirements.txt
RUN pip install --no-cache-dir -r /install/requirements.txt \
    && pip install --no-cache-dir jupyterlab

# Create user and group
RUN groupadd -g 1000 sjafari && \
    useradd -m -u 1000 -g sjafari -s /bin/bash sjafari

# Set work directory
WORKDIR /app

# Environment variables
ENV KAFKA_INSTALL_PATH /kafka/bin/

# Switch to non-root user
#USER sjafari
