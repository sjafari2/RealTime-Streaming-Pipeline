FROM sjafari2/kafkaconfluentbase:latest

# Copy consumer source code into a safe internal path
COPY --chown=sjafari:sjafari ./src/consumer/ /code/

# Switch to root to adjust permissions and install system packages
USER root

# Make sure main scripts are executable before being copied to the persistent volume
RUN chmod 755 /code/run.sh 

# Create logs folder inside target persistent volume path (used in volume mount later)
RUN mkdir -p /app/consumer-merge-data/logs && \
    chown -R 1000:1000 /app/consumer-merge-data && \
    chmod -R u+w /app/consumer-merge-data

# Install required system packages (optional if already in base image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libffi-dev \
        libssl-dev \
        libblas-dev \
        liblapack-dev \
        gfortran && \
    rm -rf /var/lib/apt/lists/*

# Return to non-root user
USER sjafari

# Keep container alive for debugging or interaction
CMD ["bash", "sleep", "infinity"]

