FROM sjafari2/kafkabase:latest

# Copy consumer code and config with correct ownership
COPY --chown=sjafari:sjafari ./src/consumer/  /app/

# Become root to install system packages and set permissions
USER root

# ✅ Install required system packages (cleaned up properly)
RUN apt-get update && apt-get install -y \
    libffi-dev \
    libssl-dev \
    libblas-dev \
    liblapack-dev \
    gfortran && \
    rm -rf /var/lib/apt/lists/*

# ✅ Ensure the script is executable
RUN chmod 755 /app/runconsumer.sh

# ✅ Optional: Debug listing of files
RUN ls -l /app

# Optional (already set in base, but explicit is fine)
USER sjafari

# ✅ Keeps the container running (debug/dev mode)
CMD ["bash", "sleep", "infinity"]

