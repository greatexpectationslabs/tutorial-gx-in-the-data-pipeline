#!/bin/bash

# Wait for the PostgreSQL database to be ready.
while ! pg_isready -h postgres -p 5432 -U airflow_user; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

# Check if the database has already been initialized
if [ ! -e /tmp/initialized ]; then
    echo "Initializing the Airflow database..."

    # Ensure the database is migrated
    airflow db migrate

    # Create default connections, roles, and users
    airflow connections create-default-connections
    airflow roles create Admin
    airflow users create --username admin --password gx --firstname Admin --lastname User --role Admin --email admin@example.com

    # Sync permissions
    airflow sync-perm

    # Mark initialization as done
    touch /tmp/initialized
    echo "Airflow initialization complete."
else
    echo "Airflow has already been initialized. Skipping DB migration and setup."
fi

# Remove stale PID file if it exists
if [ -f "/usr/local/airflow/airflow-webserver.pid" ]; then
    rm /usr/local/airflow/airflow-webserver.pid
fi

# Start the Airflow webserver and scheduler
echo "Starting Airflow webserver and scheduler..."
airflow webserver &
airflow scheduler
