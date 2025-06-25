FROM sjafari2/kafkaconfluentbase:latest

# Copy merge code into internal image path (not overwritten by mounted volume)
COPY --chown=sjafari:sjafari ./src/merge/ /code/

# Switch to root for installing system packages
USER root

RUN apt-get update && \
    apt-get install -y libopenmpi-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages globally
RUN pip install --no-cache-dir requests mpi4py

# Ensure scripts are executable
RUN chmod 755 /code/runmerge.sh && \
    chmod 755 /code/runsynthetic.sh

# Create logs folder inside target persistent volume path (used in volume mount later)
RUN mkdir -p /app/merged-data/logs && \
    chown -R 1000:1000 /app/merged-data && \
    chmod -R u+w /app/merged-data

# Drop back to non-root user
USER sjafari

CMD ["bash", "sleep", "infinity"]

