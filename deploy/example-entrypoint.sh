#!/bin/bash
set -e

echo "Starting Example ..."
sleep 3

echo "Running calibration file creator command..."
sleep 1
echo "imap-mag calibrationdemo /data/imap_mag_l1b_norm-mago_20250502_v000.cdf /data/example"
imap-mag calibrationdemo /data/imap_mag_l1b_norm-mago_20250502_v000.cdf /data/example
