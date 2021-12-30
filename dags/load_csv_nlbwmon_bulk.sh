#!/bin/bash

# Store csv path
WORKING_DIR="/home/dnieto/pruebas/nlbwmon"
# Paths for binary files
LOGGER="/usr/bin/logger"
PSQL="/usr/bin/psql"
PGHOST="192.168.11.172"
PGPORT="5432"
PGUSER="postgres"
PGPASS="raspberry"

# make sure backup directory exists
#[ ! -d $WORKING_DIR ] && mkdir -p ${WORKING_DIR}
# Get the local csv list
R_CSVs=`ls ${WORKING_DIR}`
#Remove this-day-last-week
#rm $(date +'%A')_$(date +'%F')_*
# Delete the contents of the unlogged table prior to load
$PSQL postgres://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/nlbwmon -c "TRUNCATE TABLE nlbwmon;"
# For each element in the list download the csv
for R_CSV in $R_CSVs; do
        $PSQL postgres://${PGUSER}:${PGPASS}@${PGHOST}:${PGPORT}/nlbwmon -c "\COPY nlbwmon FROM '${WORKING_DIR}/${R_CSV}' WITH DELIMITER AS ',' CSV HEADER;"
        # Log backup start time in /var/log/messages
        $LOGGER "$0: *** ${RCSV} file copied succesfully @ $(date) ***"
done