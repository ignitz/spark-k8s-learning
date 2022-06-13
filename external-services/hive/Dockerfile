FROM bitsondatadev/hive-metastore:latest

USER root
COPY postgresql-42.4.0.jar /opt/apache-hive-metastore-3.0.0-bin/lib
RUN chown hive:hive /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-42.4.0.jar

COPY entrypoint_psql.sh /entrypoint_psql.sh
RUN chown hive:hive /entrypoint_psql.sh
USER hive

ENTRYPOINT [ "/entrypoint_psql.sh" ]
