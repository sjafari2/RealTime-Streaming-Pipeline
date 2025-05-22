FROM python:3.10-slim-bullseye

# Install system packages with cleanup to reduce image size
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
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
    lsof && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Create non-root user
RUN groupadd -g 1000 sjafari && \
    useradd -m -u 1000 -g sjafari -s /bin/bash sjafari

# Set Kafka work directory and extract Kafka
WORKDIR /kafka
RUN mkdir -p /install && \
    wget -O - https://downloads.apache.org/kafka/3.8.0/kafka_2.12-3.8.0.tgz | \
    tar xzf - -C /kafka --strip-components=1 && \
    chown -R sjafari:sjafari /kafka /install

# Install base Python dependencies
COPY dockerfiles/requirements.txt /install/requirements.txt
RUN pip install --no-cache-dir -r /install/requirements.txt

# Set working directory for application layer
WORKDIR /app
COPY --chown=sjafari:sjafari ./src/pipeline-configmap.yaml .
RUN mkdir -p ./logs && chown -R sjafari:sjafari ./logs


# Environment variable for Kafka path
ENV KAFKA_INSTALL_PATH=/kafka/bin/
ENV PATH="$PATH:/home/sjafari/.local/bin"

# Default to non-root user for safety
USER sjafari

