#!/usr/bin/env bash

# Configure airflow with postgres connection string.
if [ -v AIRFLOW_POSTGRES_HOST ] && [ -v AIRFLOW_POSTGRES_PORT ] && [ -v AIRFLOW_POSTGRES_USER ] && [ -v AIRFLOW_POSTGRES_PASSWORD ]; then
    CONN="postgresql://$AIRFLOW_POSTGRES_USER:$AIRFLOW_POSTGRES_PASSWORD@$AIRFLOW_POSTGRES_HOST:$AIRFLOW_POSTGRES_PORT"
    echo "Setting AIRFLOW__CORE__SQL_ALCHEMY_CONN=${CONN}"
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$CONN
fi

if [ -v AIRFLOW__CORE__SQL_ALCHEMY_CONN ]; then
    # Wait for postgres then init the db.
    if [[ "$3" == *"webserver"* ]] || [[ "$3" == *"scheduler"* ]]; then
        HOST=`echo $AIRFLOW__CORE__SQL_ALCHEMY_CONN | awk -F@ '{print $2}'`
        FORMATTED_HOST=`echo $HOST | tr ":" " "`
        CHECK_HOST="nc -z ${FORMATTED_HOST}"

        # Sleep until we can detect a connection to host:port.
        while ! $CHECK_HOST; do
            i=`expr $i + 1`
            if [ $i -ge $CONN_ATTEMPTS ]; then
                echo "$(date) - ${HOST} still not reachable, giving up"
                exit 1
            fi
            echo "$(date) - waiting for ${HOST}... $i/$CONN_ATTEMPTS"
            sleep 10
        done

        # Ensure db initialized.
        if [[ "$3" == *"webserver"* ]] || [[ "$3" == *"scheduler"* ]]; then
            echo "Initializing airflow postgres db..."
            airflow initdb
        fi

        echo "Ensuring database..."
        sleep 5
    fi
fi

# Run the `airflow` command.
echo "Executing: $@"
exec $@
