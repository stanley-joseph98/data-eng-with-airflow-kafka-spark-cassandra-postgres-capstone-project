#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -h postgres -U airflow -d airflow; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Initialize the database if it hasn't been initialized
if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo "Initializing Airflow database..."
  airflow db init

  echo "Creating admin user..."
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the database
echo "Upgrading Airflow database..."
airflow db upgrade

# Start the webserver
echo "Starting Airflow webserver..."
exec airflow webserver