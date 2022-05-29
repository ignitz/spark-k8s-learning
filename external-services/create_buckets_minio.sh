#!/bin/bash

docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 mb s3://airflow-logs

docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 mb s3://datalake

docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 mb s3://spark-artifacts
