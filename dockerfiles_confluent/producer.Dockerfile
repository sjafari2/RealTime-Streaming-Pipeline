FROM sjafari2/kafkaconfluentbase:latest

# Copy producer code to /code (bootstrapped later to persistent volume)
COPY --chown=sjafari:sjafari ./src/producer/ /code/

# Copy config files to /defaults
COPY --chown=sjafari:sjafari ./src/config/ /defaults/

# Switch to root to adjust script permissions
USER root

# Make critical scripts executable
RUN chmod 755 /code/runproducer.sh && \
    chmod 755 /code/runsynthetic.sh

# Create logs folder inside target persistent volume path (used in volume mount later)
RUN mkdir -p /app/producer-data/logs && \
    chown -R 1000:1000 /app/producer-data && \
    chmod -R u+w /app/producer-data

# Return to non-root user for security
USER sjafari

# Install producer-specific Python dependencies
RUN pip install --no-cache-dir nltk && \
    python3 -m nltk.downloader stopwords && \
    python3 -m spacy download en_core_web_sm

# Optional: debug listing
# RUN ls -lR /code /app

# Keep the container alive for manual interaction
CMD ["bash", "sleep", "infinity"]

