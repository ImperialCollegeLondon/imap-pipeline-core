#!/bin/bash
set -e

echo "Starting IMAP MAG pipeline..."

imap-db create-db

imap-db upgrade-db

echo "DB admin complete"

# delete all data
echo "Deleting all data - reset the datastore as this is just a test"
rm -rf /data/hk_l0
rm -rf /data/hk_l1
rm -rf /data/science
rm -rf /data/output

START_DATE='2025-05-02'
END_DATE='2025-05-03'

#if arg 0 is set, use it as the start date
if [ -n "$1" ]; then
    START_DATE=$1
fi
if [ -n "$2" ]; then
    END_DATE=$2
fi

echo "Running pipeline for $START_DATE to $END_DATE"

imap-mag fetch-binary --apid 1063 --start-date $START_DATE --end-date $END_DATE

imap-mag process power.pkts

imap-mag fetch-science --level l1b --start-date $START_DATE --end-date $END_DATE

imap-db query-db

imap-mag calibrate --date 2025-10-31 --method noop --sensor MAGo --mode norm

ls -l /data
