FROM sjafari2/kafkabase:latest

WORKDIR /app
COPY ./src/consumer .
COPY ./src/run-jupyterlab.sh .
COPY ./src/pipeline-configmap.yaml .

# No additional unique dependencies, but the file setup might differ
RUN chown -R sjafari:sjafari /app && chmod 755 /app/runconsumer.sh

ENV KAFKA_INSTALL_PATH /kafka/bin/
USER sjafari
CMD ["bash", "sleep infinity"]
