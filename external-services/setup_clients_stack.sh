#!/bin/bash
(
    docker compose -f docker-compose.clients.yaml up -d
) && (
    docker compose -f docker-compose.clients.yaml exec superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin && \
    docker compose -f docker-compose.clients.yaml exec superset superset db upgrade && \
    docker compose -f docker-compose.clients.yaml exec superset superset init
) && echo "Clients started."
