FROM sjafari2/kafkabase:latest

WORKDIR /app
COPY ./src/request .
COPY ./src/run-jupyterlab.sh .
COPY ./src/pipeline-configmap.yaml .

# Install Python dependencies unique to the request service
RUN pip install --no-cache-dir Flask uvicorn fastapi

# Additional setup specific to request service
RUN mkdir -p ./request-data && chown -R sjafari:sjafari /app && chmod 755 /app/runrequest.sh

ENV KAFKA_INSTALL_PATH /kafka/bin/
USER sjafari
CMD ["bash", "sleep infinity"]
