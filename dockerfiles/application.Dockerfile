FROM sjafari2/kafkabase:latest

WORKDIR /app
COPY ./src/application .
#COPY ./src/run-jupyterlab.sh .
COPY ./src/pipeline-configmap.yaml .

# Install additional unique dependencies for the application service
RUN apt-get update && apt-get install -y \
    openmpi-bin \
    libopenmpi-dev \
    mpich \
    libpcap-dev

RUN pip install --no-cache-dir mpi4py \
    && python -m spacy download en_core_web_sm \
    && python -m nltk.downloader stopwords

RUN chown -R sjafari:sjafari /app && chmod 755 runapplication.sh

CMD ["bash", "sleep infinity"]
