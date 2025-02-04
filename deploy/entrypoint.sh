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

imap-mag fetch-binary --config config-hk-download.yaml --apid 1063 --start-date $START_DATE --end-date $END_DATE

imap-mag process --config config-hk-process.yaml power.pkts

imap-mag fetch-science --level l1b --start-date $START_DATE --end-date $END_DATE --config config-sci.yaml

imap-db query-db

imap-mag calibrate --config calibration_config.yaml --method SpinAxisCalibrator imap_mag_l1b_norm-mago_20250511_v000.cdf

imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1b_norm-mago_20250511_v000.cdf

ls -l /data


