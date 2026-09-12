FROM sjafari2/kafkaconfluentbase:latest

# Copy controller code to /code (bootstrapped later to persistent volume)
COPY --chown=sjafari:sjafari ./src/controller/ /code/

# Copy config files to /defaults
COPY --chown=sjafari:sjafari ./src/pipeline-configmap.yaml /defaults/pipeline-configmap.yaml
RUN chmod 644 /defaults/pipeline-configmap.yaml

# Switch to root to adjust script permissions
USER root

# Make critical scripts executable
RUN chmod 755 /code/run_lag_controller.sh && \
    chmod 755 /code/lag_controller.py

# Create logs folder inside target persistent volume path (used in volume mount later)
RUN mkdir -p /app/controller-data/logs && \
    chown -R 1000:1000 /app/controller-data && \
    chmod -R u+w /app/controller-data

# Return to non-root user for security
USER sjafari

WORKDIR /code

# Environment defaults (can be overridden from Deployment)
ENV PIPELINE_CONFIG_PATH=/config/pipeline-configmap.yaml
ENV LOG_LEVEL=INFO

# Optional: debug listing
# RUN ls -lR /code /app

# Keep the container alive for manual interaction
#CMD ["bash", "sleep", "infinity"]

# Run the controller loop
ENTRYPOINT ["/bin/bash", "/code/run_lag_controller.sh"]
