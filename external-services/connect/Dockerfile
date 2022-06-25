FROM confluentinc/cp-kafka-connect-base:7.1.0

ARG DEBEZIUM_VERSION="1.9.2"

# Postgres connnector
# https://www.confluent.io/hub/debezium/debezium-connector-postgresql
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:${DEBEZIUM_VERSION}
# MySQL connector
# https://www.confluent.io/hub/debezium/debezium-connector-mysql
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:${DEBEZIUM_VERSION}
# SQL Server connector
# https://www.confluent.io/hub/debezium/debezium-connector-sqlserver
RUN confluent-hub install --no-prompt debezium/debezium-connector-sqlserver:${DEBEZIUM_VERSION}
# MongoDB connector 
# https://www.confluent.io/hub/debezium/debezium-connector-mongodb
RUN confluent-hub install --no-prompt debezium/debezium-connector-mongodb:${DEBEZIUM_VERSION}
