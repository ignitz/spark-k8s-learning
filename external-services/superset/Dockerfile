FROM apache/superset:1.5.1
# Switching to root to install the required packages
USER root
# Example: installing the MySQL driver to connect to the metadata database
# if you prefer Postgres, you may want to use `psycopg2-binary` instead
RUN pip install mysqlclient
# TODO: Update to trino package
RUN pip install sqlalchemy-trino
# Switching back to using the `superset` user
USER superset
