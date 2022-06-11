#!/bin/bash

kubectl create namespace spark --dry-run=client -o yaml | kubectl apply -f -

# Create namespace "spark" where will run Jobs
kubectl create namespace spark --dry-run=client -o yaml | kubectl apply -f -

# Create service account "spark" to spark-operator allow submit jobs
kubectl create serviceaccount spark -n spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark:spark --namespace=spark

# Install spark operator
helm upgrade --install spark-operator spark-operator/spark-operator --debug \
    --namespace spark-operator \
    --create-namespace \
    -f spark/operator/values.yaml