FROM python:3.10-slim-bullseye

# Install system packages including librdkafka for confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    liblz4-dev \
    libsnappy-dev \
    librdkafka-dev \
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
    iputils-ping \
    dnsutils \
    netcat \
    telnet \
    procps \
    curl \
    lsof && \
    rm -rf /var/lib/apt/lists/*

# Install yq - Mike Farah version (for YAML parsing)
RUN curl -L https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 \
    -o /usr/bin/yq && \
    chmod +x /usr/bin/yq && \
    yq --version

# Upgrade pip and install basic Python build tools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Create a non-root user
RUN groupadd -g 1000 sjafari && \
    useradd -m -u 1000 -g sjafari -s /bin/bash sjafari

# Install Python requirements (shared by child images)
COPY dockerfiles_confluent/requirements.txt /install/requirements.txt
RUN pip install --no-cache-dir -r /install/requirements.txt

# Download and install Apache Kafka
WORKDIR /kafka
RUN mkdir -p /install && \
    wget -O - https://downloads.apache.org/kafka/3.8.0/kafka_2.12-3.8.0.tgz | \
    tar xzf - -C /kafka --strip-components=1 && \
    chown -R sjafari:sjafari /kafka /install

# Create writable working directories for derived images
RUN mkdir -p /app /code /defaults && \
    chown -R sjafari:sjafari /app /code /defaults && \
    chmod -R u+w /app /code /defaults

# Copy config files to /defaults
#COPY --chown=sjafari:sjafari ./src/config/ /defaults/

# Set Kafka path and local pip path
ENV KAFKA_INSTALL_PATH=/kafka/bin/
ENV PATH="$PATH:/home/sjafari/.local/bin:$PATH"

# Switch to the non-root user
USER sjafari

# Set working directory for containers built from this base
WORKDIR /app

