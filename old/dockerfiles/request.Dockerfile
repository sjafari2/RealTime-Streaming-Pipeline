FROM sjafari2/kafkabase:latest

# Copy all required files and folders to /app
COPY --chown=sjafari:sjafari ./src/request/ /app/
COPY --chown=sjafari:sjafari ./data/simulator/ /app/request-data/

# Install Python dependencies
RUN pip install --no-cache-dir Flask uvicorn fastapi

# Make startup scripts executable
RUN chmod 755 /app/runrequest.sh && \
    chmod 755 /app/runuvicorn.sh

# Debug listing
RUN ls -l /app

CMD ["bash", "sleep", "infinity"]

