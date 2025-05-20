FROM sjafari2/kafkabase:latest

# Copy code and configs
COPY --chown=sjafari:sjafari ./src/merge/ /app/

# Become root to install packages and set permissions
USER root

RUN apt-get update && \
    apt-get install -y libopenmpi-dev && \
    rm -rf /var/lib/apt/lists/*

# Install required Python packages globally (as root is fine)
RUN pip install --no-cache-dir requests mpi4py

# Ensure your script is executable (use full path)
RUN chmod 755 /app/runmerge.sh

# Optional: check contents of /app
RUN ls -l /app

# Drop to non-root user
USER sjafari

CMD ["bash", "sleep", "infinity"]

