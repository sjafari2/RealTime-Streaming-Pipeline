FROM sjafari2/kafkabase:latest

# Copy producer code and configuration files
COPY --chown=sjafari:sjafari ./src/producer/ /app/

# Temporarily switch to root to update permissions
USER root
RUN chmod 755 /app/runproducer.sh
USER sjafari

# Install producer-specific Python packages
RUN pip install --no-cache-dir nltk && \
    python3 -m spacy download en_core_web_sm && \
    python3 -m nltk.downloader stopwords

# Optional: list files for debug
RUN ls -l /app

# Keep container alive (debug/dev mode)
CMD ["bash", "sleep", "infinity"]

