FROM sjafari2/kafkabase:latest

# Specific steps for producer, if any
COPY ./src/producer /app
#COPY ./src/run-jupyterlab.sh /app
COPY ./src/pipeline-configmap.yaml /app

# Specific Python packages not included in the base image
 RUN pip install nltk \
     && python3 -m spacy download en_core_web_sm \
     && python3 -m nltk.downloader stopwords

# Set ownership and permissions
RUN chown -R sjafari:sjafari /app \
    && chmod 755 /app/runproducer.sh

CMD ["bash", "sleep infinity"]
