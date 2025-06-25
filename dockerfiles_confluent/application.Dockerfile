FROM sjafari2/kafkaconfluentbase:latest

# Copy application code into a safe internal path
COPY --chown=sjafari:sjafari ./src/application/ /code/

# Become root to install system dependencies and set permissions
USER root

# Make scripts executable
RUN chmod 755 /code/runapplication.sh && \
    chmod 755 /code/runsynthetic.sh

# Create logs folder inside target persistent volume path (used in volume mount later)
RUN mkdir -p /app/app-merge-data/logs && \
    chown -R 1000:1000 /app/app-merge-data && \
    chmod -R u+w /app/app-merge-data

# Install system-level dependencies for MPI and networking
RUN apt-get update && \
    apt-get install -y \
        openmpi-bin \
        libopenmpi-dev \
        libpcap-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages (requires root for full download access)
RUN pip install --no-cache-dir mpi4py && \
    python -m spacy download en_core_web_sm && \
    python -m nltk.downloader stopwords

# Drop back to non-root user for runtime
USER sjafari

# Default CMD (development/debug mode)
CMD ["bash", "sleep", "infinity"]

