#!/bin/bash

# Create directories
echo "Creating directories..."
mkdir -p data logs plugins

# Generate secure passwords
echo "Generating secure passwords..."
POSTGRES_PASSWORD=$(LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 32)
CLICKHOUSE_PASSWORD=$(LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 32)
AIRFLOW_PASSWORD=$(LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 32)

# Create .env file
echo "Creating .env file with new passwords..."
cat <<EOT > .env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=airflow
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD}
AIRFLOW_UID=50000
EOT

echo ".env file created successfully."
echo "--- "
echo "IMPORTANT: Your generated passwords are listed below. They have been saved to the .env file and will not be shown again."
echo "Postgres Password: ${POSTGRES_PASSWORD}"
echo "ClickHouse Password: ${CLICKHOUSE_PASSWORD}"
echo "Airflow Password: ${AIRFLOW_PASSWORD}"
echo "--- "

# Run docker-compose
echo "Running docker-compose..."
docker-compose up -d

echo "Setup complete."