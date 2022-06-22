#!/bin/bash

mkdir -pv build/

zip -r build/jibaro.zip jibaro

docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    -v $PWD/build:/build:ro \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 cp --recursive /build s3://spark-artifacts/lib

docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    -v $PWD/scripts:/scripts:ro \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 cp --recursive /scripts s3://spark-artifacts/pyspark/

docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    -v $PWD/airflow/dags:/airflow/dags:ro \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 sync /airflow/dags s3://airflow-artifacts/dags/ --exclude "*.pyc" --delete
