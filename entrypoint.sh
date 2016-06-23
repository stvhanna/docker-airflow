#!/usr/bin/env bash

CMD="airflow"

# Configure airflow with postgres connection string.
if [ -v AIRFLOW_POSTGRES_HOST ] && [ -v AIRFLOW_POSTGRES_USER ] && [ -v AIRFLOW_POSTGRES_PASSWORD ]; then
    echo "Querying for postgres host SRV record (${AIRFLOW_POSTGRES_HOST}) ..."

    DNS=`dig $AIRFLOW_POSTGRES_HOST +noall +answer +short -t SRV`
    CONN=`echo $DNS | awk -v user=$AIRFLOW_POSTGRES_USER -v pass=$AIRFLOW_POSTGRES_PASSWORD '{print "postgresql://" user ":" pass "@" $4 ":" $3}'`

    echo "Setting AIRFLOW__CORE__SQL_ALCHEMY_CONN=${CONN}"

    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$CONN
fi

# Ensure db initialized.
if [ "$1" = "webserver" ]; then
    echo "Initializing airflow postgres db..."
    $CMD initdb
    sleep 5
fi

# Run the `airflow` command.
COMMAND="$CMD $@"
echo "Executing: $COMMAND"
exec $COMMAND
