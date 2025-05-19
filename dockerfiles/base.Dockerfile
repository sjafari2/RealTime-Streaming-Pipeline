FROM python:3.7-slim-bullseye

# Install system packages
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    openjdk-17-jdk \
    bash \
    wget \
    pkg-config \
    libhdf5-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    libatlas-base-dev \
    nano \
    vim \
    screen \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip setuptools wheel

# Create user and group before chown
RUN groupadd -g 1000 sjafari && \
    useradd -m -u 1000 -g sjafari -s /bin/bash sjafari

# Set Kafka work directory and extract Kafka
WORKDIR /kafka
RUN mkdir -p /install && \
    wget -O - https://downloads.apache.org/kafka/3.8.0/kafka_2.12-3.8.0.tgz | tar xzf - -C /kafka --strip-components=1 && \
    chown -R sjafari:sjafari /kafka /install

# Install Python dependencies
COPY dockerfiles/requirements.txt /install/requirements.txt
RUN pip install --no-cache-dir -r /install/requirements.txt \
    && pip install --no-cache-dir jupyterlab

# Set working directory for your app
WORKDIR /app

# Set environment variables
ENV KAFKA_INSTALL_PATH /kafka/bin/

# Switch to non-root user
USER sjafari

