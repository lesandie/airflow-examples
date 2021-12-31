#!/bin/bash
set -eo pipefail

# Store csv path
declare WORKING_DIR="/home/dnieto/pruebas/nlbwmon/mod"
# Paths for binary files
declare LOGGER="/usr/bin/logger"
declare PSQL="/usr/bin/psql"
declare PGHOST="192.168.11.172"
declare PGPORT="5432"
declare PGUSER="postgres"
declare PGPASS="raspberry"
# make sure backup directory exists
#[ ! -d $WORKING_DIR ] && mkdir -p ${WORKING_DIR}
# Get the local csv list
declare R_CSVs=`ls ${WORKING_DIR}`

#Remove this-day-last-week
#rm $(date +'%A')_$(date +'%F')_*

# Delete the contents of the unlogged table prior to load
"$PSQL" postgres://"$PGUSER":"$PGPASS"@"$PGHOST":"$PGPORT"/nlbwmon -c "TRUNCATE TABLE nlbwmon;"

# For each element in the list download the csv
for R_CSV in "$R_CSVs"; do
        "$PSQL" postgres://"$PGUSER":"$PGPASS"@"$PGHOST":"$PGPORT"/nlbwmon -c \
        "\COPY nlbwmon (family,proto,port,mac,ip,conns,rx_bytes,rx_pkts,tx_bytes,tx_pkts,layer7,month) FROM '${WORKING_DIR}/${R_CSV}' WITH DELIMITER AS ',' CSV HEADER;"
        # Log backup start time in /var/log/messages
        "$LOGGER" "${0}: *** ${RCSV} file copied succesfully @ $(date) ***"
done