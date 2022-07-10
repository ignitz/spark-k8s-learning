#!/bin/bash

buckets=(
    airflow-logs
    spark-artifacts
    spark-control
    spark-history
    datalake-raw 
    datalake-staged
    datalake-curated
)

for bucket_name in "${buckets[@]}"
do
    docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    amazon/aws-cli:latest \
    --endpoint-url http://minio:9000 --region us-east-1 s3 mb s3://$bucket_name || true
done

# Create s3://spark-history/logs folder
docker run --rm \
    -e AWS_ACCESS_KEY_ID=minio \
    -e AWS_SECRET_ACCESS_KEY=miniominio \
    --network kind \
    --entrypoint bash \
    amazon/aws-cli:latest \
    -c "touch /tmp/dummy && aws --endpoint-url http://minio:9000 --region us-east-1 s3 cp /tmp/dummy s3://spark-history/logs/dummy"
