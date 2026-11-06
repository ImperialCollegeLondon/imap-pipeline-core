# Build and run

Example matlab with docker can be built with:

`docker build -f deploy/MATLAB-Dockerfile --build-arg USERID=1000 -t testing .`

and run with

`docker run -v $(pwd)/tests/test_data:/data -e MLM_LICENSE_FILE={MLM_LICENSE_FILE} testing`

to create a CDF in `tests/test_data`

## NOAA wind database update

For existing Crump target databases, run `deploy/relax_noaa_wind_nullability.sql`
with `psql` against the upload target database. New tables already use the nullable
schema from `imap-db-ingest-config.yaml`. After applying the script, rerun the
PostgreSQL upload flow with `find_files_after` set before the affected files'
last-modified times so that previously failed files are retried.
