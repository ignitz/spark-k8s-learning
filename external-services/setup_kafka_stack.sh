#!/bin/bash
(
    cd external-services && docker compose up -d && \
    sleep 10 && \
    timeout 90s bash -c "until docker exec postgres pg_isready ; do sleep 5 ; done" && \
    (
        docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;";
        docker exec postgres psql -U postgres -c "CREATE DATABASE metastore;"
    ) || true
) && echo "Started... Waiting 30 seconds to make sure everything is working" && \
sleep 30 && \
(
    cd external-services && docker compose up -d
) && (
    cd external-services && bash create_buckets_minio.sh
) && (
    cd external-services && \
    bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8083)" != "200" ]]; do sleep 5; done' && \
    bash create_connector.sh
) && (
    cd external-services && \
    docker compose exec superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin && \
    docker compose exec superset superset db upgrade && \
    docker compose exec superset superset init
) && echo "Done"
