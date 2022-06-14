#!/bin/bash

kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic airflow-secret -n airflow --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
helm upgrade --install airflow --debug --create-namespace \
    apache-airflow/airflow --namespace airflow \
    --version v1.6.0 -f values.yaml
