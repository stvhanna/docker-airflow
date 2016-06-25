#!/usr/bin/env bash

CMD="airflow"
CONN_ATTEMPTS=10

# Configure airflow with postgres connection string.
if [ -v AIRFLOW_POSTGRES_HOST ] && [ -v AIRFLOW_POSTGRES_USER ] && [ -v AIRFLOW_POSTGRES_PASSWORD ]; then
    echo "Querying for postgres host SRV record (${AIRFLOW_POSTGRES_HOST}) ..."

    DNS=`dig $AIRFLOW_POSTGRES_HOST +noall +answer +short -t SRV`
    CONN=`echo $DNS | awk -v user=$AIRFLOW_POSTGRES_USER -v pass=$AIRFLOW_POSTGRES_PASSWORD '{print "postgresql://" user ":" pass "@" $4 ":" $3}'`

    echo "Setting AIRFLOW__CORE__SQL_ALCHEMY_CONN=${CONN}"

    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$CONN
fi

# Wait for postgres then init the db.
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ]; then
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
        sleep 5
    done

    # Ensure db initialized.
    if [ "$1" = "webserver" ]; then
        echo "Initializing airflow postgres db..."
        $CMD initdb
    fi

    # Give initdb some time to run.
    echo "Waiting for airflow database to be initialized..."
    sleep 5
fi


# Run the `airflow` command.
COMMAND="$CMD $@"
echo "Executing: $COMMAND"
exec $COMMAND
