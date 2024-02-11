FROM sjafari2/kafkabase:latest

WORKDIR /app
COPY ./src/merge .
COPY ./src/run-jupyterlab.sh .
COPY ./src/pipeline-configmap.yaml .

# Install any additional unique Python dependencies for the merge service
RUN pip install --no-cache-dir requests mpi4py

RUN chown -R sjafari:sjafari /app && chmod 755 runmerge.sh

CMD ["bash", "sleep infinity"]
