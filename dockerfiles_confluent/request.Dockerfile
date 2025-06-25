FROM sjafari2/kafkaconfluentbase:latest

# Copy request application code into an internal safe image path
COPY --chown=sjafari:sjafari ./src/request/ /code/
COPY --chown=sjafari:sjafari ./data/simulator/ /code/data/

# Become root to install dependencies and set permissions
USER root

# Ensure scripts are executable
RUN chmod 755 /code/runrequest.sh && \
    chmod 755 /code/runuvicorn.sh


# Create logs folder inside target persistent volume path (used in volume mount later)
RUN mkdir -p /app/request-data/logs && \
    chown -R 1000:1000 /app/request-data && \
    chmod -R u+w /app/request-data

# Install Python packages required by the request API
RUN pip install --no-cache-dir Flask uvicorn fastapi

# Drop back to application user
USER sjafari

# Default CMD (development/debug mode)
CMD ["bash", "sleep", "infinity"]

