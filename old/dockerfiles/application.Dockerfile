FROM sjafari2/kafkabase:latest

# Copy application code
COPY --chown=sjafari:sjafari ./src/application/ /app/

# Become root to install system dependencies and set permissions
USER root

RUN apt-get update && apt-get install -y \
    openmpi-bin \
    libopenmpi-dev \
    libpcap-dev && \
    rm -rf /var/lib/apt/lists/*

# Make sure the script is executable before dropping privileges
RUN chmod 755 /app/runapplication.sh

# Optional: debug
RUN ls -l /app

# Drop to non-root
USER sjafari

# Python-level dependencies (as non-root is okay here)
RUN pip install --no-cache-dir mpi4py && \
    python -m spacy download en_core_web_sm && \
    python -m nltk.downloader stopwords

CMD ["bash", "sleep", "infinity"]

