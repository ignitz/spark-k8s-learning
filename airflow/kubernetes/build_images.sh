#!/bin/bash

(
    cd ../docker/airflow && \
    docker build -f Dockerfile -t ignitz/airflow:latest .
)
(
    cd ../docker/s3sync && \
    docker build -f Dockerfile -t ignitz/s3sync:latest .
)

kind load docker-image ignitz/airflow
kind load docker-image ignitz/s3sync
