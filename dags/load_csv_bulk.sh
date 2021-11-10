#!/bin/bash

# Store csv path
WORKING_DIR="/home/dnieto/pruebas"
# Paths for binary files
LOGGER="/usr/bin/logger"
PSQL="/usr/bin/psql"
PGHOST="localhost"
PGPORT="5432"
PGUSER="airflow"
PGPASS="airflow"

# make sure backup directory exists
#[ ! -d $WORKING_DIR ] && mkdir -p ${WORKING_DIR}
# Get the local csv list
R_CSVs=`ls ${WORKING_DIR}/yellow_tripdata_2019-01.csv`
#Remove this-day-last-week
#rm $(date +'%A')_$(date +'%F')_*
# Delete the contents of the unlogged table prior to load
$PSQL postgres://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/nyc_taxi -c "TRUNCATE TABLE csv_load;"
# For each element in the list download the csv
for R_CSV in $R_CSVs; do
        $PSQL postgres://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/nyc_taxi -c "\COPY csv_load FROM '${R_CSV}' WITH DELIMITER AS ',' CSV HEADER;"
        # Log backup start time in /var/log/messages
        $LOGGER "$0: *** ${RCSV} file copied succesfully @ $(date) ***"
done