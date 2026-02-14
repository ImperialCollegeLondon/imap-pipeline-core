# IMAP Pipeline Core

IMAP Pipeline Core is a Python data processing pipeline for the IMAP (Interstellar Mapping and Acceleration Probe) mission. It processes magnetometer data from multiple sources (WebPODA, IMAP SDC CDF files API, I-ALiRT API), performs calibration, and manages a  shared folder of file saved in a data store. The data-store data is tracked in a postgres database which also contains some packet data mirrored from CSV and CDF files.

The project uses Prefect for workflow orchestration, Typer for CLI commands, SQLAlchemy ORM with Alembic for database management, and Pydantic for configuration management. It includes a magnetometer calibration toolkit and supports automated testing with pytest.

## Developer setup steps - option 1: Dev Container

clone, install vscode, install docker desktop.

Open Dev Container in Visual Studio Code. Requires the [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension.

The container has all your tools installed and ready to go. You can run the tests, build the package, and run the CLI from the terminal in the container.

To use WebPODA APIs, an access token needs to be defined in the environment as `IMAP_WEBPODA_TOKEN`. If this variable exists in WSL's `~/.bashrc` or `~/.zshrc`, then this will be automatically copied over to the Dev Container. The access token needs to be defined as an encrypted string, as explained on the [WebPODA documentation](https://lasp.colorado.edu/ops/imap/poda/#auth).

## Developer setup steps - option 2 manual linux/WSL Setup

1. [Download and install Poetry](https://python-poetry.org/docs/#installation) following the instructions for your OS.
2. Set up the virtual environment:

    ```bash
    poetry install
    ```

3. Activate the virtual environment (alternatively, ensure any python-related command is preceded by `poetry run`):

    ```bash
    poetry shell
    ```

4. Install the git hooks:

    ```bash
    pre-commit install
    ```

5. To use the docker /data mount you need a folder on your WSL and a user with a given UID

```bash
# in WSL on your HOST
mkdir -p /mnt/imap-data
sudo adduser -u $IMAP_USERID --disabled-password --gecos "" $IMAP_USERNAME
# you have created the user with the same UID as in the container. now grant the folder to the user
chown -R $IMAP_USERNAME:$IMAP_USERNAME /mnt/imap-data
```

## Build, pack and test

```bash
./build.sh
./pack.sh
```

You can also build a docker image with `./build-docker.sh`

## Using the CLI inside the docker container

```bash
# Using imap-mag CLI:
docker run --entrypoint /bin/sh ghcr.io/imperialcollegelondon/imap-pipeline-core:local-dev -c "imap-mag hello world"

# Using the prefect CLI:
docker run --entrypoint /bin/bash -it --rm -e PREFECT_API_URL=http://prefect:4200/api --network mag-lab-data-platform ghcr.io/imperialcollegelondon/imap-pipeline-core:local-dev -c "prefect --version"


### Deploy to a full Prefect server using a docker container (e.g. from WSL)

From a linux host or WSL (i.e. not in a dev container) you can use the container image to run a deployment:

```bash

./pack.sh
./build-docker.sh

source dev.env

docker run -it --rm \
    --network mag-lab-data-platform \
    --env-file defaults.env \
    --env-file dev.env \
    --entrypoint /bin/bash \
    ghcr.io/imperialcollegelondon/imap-pipeline-core:local-dev \
    -c "python -c 'import prefect_server.workflow; prefect_server.workflow.deploy_flows()'"
```

## Get the prefect server running in a local dev env

**EITHER** Use the dev_start.sh script to start a local prefect server and a local postgres database for the imap app to use.
**OR** use the vscode task `start_servers` that does this for you - see [.vscode/tasks.json](.vscode/tasks.json)
**OR** do it manually as below:

```bash
# in the root of the repo, start a local dev prefect server in a terminal
poetry install
source .venv/bin/activate
prefect server start

# in another terminal start a database for the imap app to use (add -d for detached mode)
docker run --name postgres_imap_dev -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DATABASE=imap -p 5432:5432 postgres:17-alpine

# in a third terminal, deploy the imap flows
source .venv/bin/activate
cp defaults.env .env
source .env
python -c 'import prefect_server.workflow; prefect_server.workflow.deploy_flows(local_debug=True)'
# Or perhaps PYTHONPATH=src:$PYTHONPATH python -m prefect_server.workflow --local

# Now open the UI in a browser at http://127.0.0.1:4200/deployments
# Go to the blocks page and make sure to add any credentials such as the web poda auth code
```

## Debugging a prefect flow

This is a the same as the above but instead of calling prefect_server.workflow.deploy_flows in the CLI above, you can use the launch profile "Prefect deploy and run" to do the same thing in vscode with a debugger attached and then run your flow from there.

You can also setup flows to just deploy one at a time for debugging and then use "debug current file". Run F5 inside spiceDownloadFlow.py for example to debug that flow and see the code in the `if __main__` block at the bottom run.

## CLI Commands

All core functionality and logic should be available as simnple CLI commands as well as the usual prefect based flows.

### Fetch I-ALiRT

```bash
export IALIRT_API_KEY=[YOUR_SECRET_HERE!]
imap-mag fetch ialirt --start-date 2025-10-02 --end-date '2025-10-03 23:59:59'
```

### Fetch Binary HK from WebPODA

```bash
export IMAP_WEBPODA_TOKEN=[YOUR_SECRET_HERE!]
imap-mag fetch binary --apid 1063 --start-date 2025-01-02 --end-date 2025-01-03
imap-mag fetch binary --packet SID3_PW --start-date 2025-01-02 --end-date 2025-01-03
imap-mag fetch binary --packet SID3_PW --start-date 2025-01-02 --end-date 2025-01-03 --ert
```

### Fetch Science CDFs from SDC

```bash
export IMAP_API_KEY=[YOUR_SECRET_HERE!]
imap-mag fetch science --level l1b --modes burst --start-date 2025-01-02 --end-date 2025-01-03
imap-mag fetch science --level l2 --modes norm --frame dsrf --start-date 2025-01-02 --end-date 2025-01-03
imap-mag fetch science --level l2 --modes norm --frame dsrf --start-date 2025-01-02 --end-date 2025-01-03 --ingestion-date
```

### Process Binary HK to CSV

```bash
imap-mag process data/hk/mag/l0/hsk-pw/2025/01/imap_mag_l0_hsk-pw_20250102_v000.pkts
```

### Publish Calibration to SDC

```bash
export IMAP_API_KEY=[YOUR_SECRET_HERE!]
imap-mag publish imap_mag_l2-norm-offsets_20250102_20250102_v001.cdf
```

## Using crump to import data

Example 1 - generate a config file for multiple files:

```bash
crump prepare -c imap-db-ingest-config.yaml ./tests/datastore/hk/**/*.csv
```

Example 2 - prepare a section in config file based on a data file:

```bash
crump prepare -c imap-db-ingest-config.yaml --job-name imap_sc_l1_x286_v ./tests/datastore/hk/**/imap_sc_l1_x286_20251109_v001.csv
```

Example 3 - crump command to ingest data based on a config file section:

```bash
# remove dry-run if you are happy with results!
    DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/imap \
        crump sync --job imap_mag_l1_hsk-procstat_v --dry-run  ./tests/datastore/hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v*.csv imap-db-ingest-config.yaml

    crump sync --job imap_mag_l1_hsk-pw_v --dry-run  ./tests/datastore/hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251101_v*.csv imap-db-ingest-config.yaml

    crump sync --job imap_mag_l1_hsk-sci_v --dry-run  ./tests/datastore/hk/mag/l1/hsk-sci/2025/12/imap_mag_l1_hsk-sci_20251206_v001.csv  imap-db-ingest-config.yaml

    crump sync --job imap_mag_l1_hsk-sid15_v  ./tests/datastore/hk/mag/l1/hsk-sid15/2025/11/imap_mag_l1_hsk-sid15_20251101_v001.csv imap-db-ingest-config.yaml

    crump sync -- job imap_mag_l1_hsk-status_v  ./tests/datastore/hk/mag/l1/hsk-status/2025/11/imap_mag_l1_hsk-status_20251101_v001.csv imap-db-ingest-config.yaml
```
