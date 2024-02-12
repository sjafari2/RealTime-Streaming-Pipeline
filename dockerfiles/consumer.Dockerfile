FROM sjafari2/kafkabase:latest

WORKDIR /app
COPY ./src/consumer .
#COPY ./src/run-jupyterlab.sh .
COPY ./src/pipeline-configmap.yaml .


RUN apt-get update && apt-get install -y \
    libffi-dev \
    libssl-dev \
    libblas-dev \
    liblapack-dev \
    gfortran 

RUN chown -R sjafari:sjafari /app && chmod 755 /app/runconsumer.sh

ENV KAFKA_INSTALL_PATH /kafka/bin/
USER sjafari
CMD ["bash", "sleep infinity"]
