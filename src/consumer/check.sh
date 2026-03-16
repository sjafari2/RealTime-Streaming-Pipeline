#!/bin/bash

mount | grep -E '/config|pipeline-configmap'
# or
df -T /config
ls -l /config/pipeline-configmap.yaml
stat -f -c '%T' /config 2>/dev/null || df -T /config

