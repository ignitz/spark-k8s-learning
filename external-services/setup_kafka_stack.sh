#!/bin/bash
(
    cd external-services && docker compose up -d && \
    sleep 10 && \
    timeout 90s bash -c "until docker exec postgres pg_isready ; do sleep 5 ; done" && \
    docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;"
) && echo "Started... Waiting 30 seconds to make sure everything is working" && \
sleep 30 && \
(
    cd external-services && docker compose up -d
) && (
    cd external-services && bash create_connector.sh
) && (
    cd external-services && bash create_buckets_minio.sh
) && echo "Done"