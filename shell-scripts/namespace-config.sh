#!/bin/sh
kubectl config use-context nautilus
kubectl config set-context $(kubectl config current-context) --namespace=kafkastreamingdata
