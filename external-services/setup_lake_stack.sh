#!/bin/bash
(
    docker compose -f docker-compose.lake.yaml up -d && \
    sleep 10 && \
    timeout 90s bash -c "until docker exec postgres pg_isready ; do sleep 5 ; done" && \
    (
        (
            docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;" && \
            echo "Database ``airflow`` created."
        ); (
            docker exec postgres psql -U postgres -c "CREATE DATABASE metastore;" && \
            echo "Database ``metastore`` created."
        )
    ) || true
) && echo "Started... Waiting 30 seconds to make sure everything is working" && \
sleep 30 && \
(
    docker compose -f docker-compose.lake.yaml up -d
) && (
    bash create_buckets_minio.sh
) && (
    bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8083)" != "200" ]]; do sleep 5; done' && \
    bash create_connector.sh
) && echo "Lake started."
