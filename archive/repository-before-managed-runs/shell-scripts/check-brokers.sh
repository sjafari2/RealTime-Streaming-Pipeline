#!/bin/bash
docker run --rm edenhill/kcat:1.7.0 \
  -b pip-kafka-controller-0.pip-kafka-controller-headless.kafkastreamingdata.svc.cluster.local:9092 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanism=PLAIN \
  -X sasl.username=user1 \
  -X sasl.password=5x4XjjbPod \
  -L

