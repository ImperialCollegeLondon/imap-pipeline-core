#!bash

# Start a development environment for the project
# launch a prefect server
# deploy the project to the server
#start any other services like wiremock

set -e

export DEV_SERVER=127.0.0.1
export DEV_SERVER_PORT=4200

export SQLALCHEMY_URL=postgresql+psycopg://postgres:postgres@host.docker.internal:5432/imap

export IMAP_DATA_FOLDER=tests/datastore
export IMAP_WEBSITE_HOST=localhost

export PREFECT_SERVER_LOGGING_LEVEL="INFO"

echo "Configure prefect server to run at http://$DEV_SERVER:$DEV_SERVER_PORT"
prefect config set PREFECT_API_URL="http://$DEV_SERVER:$DEV_SERVER_PORT/api"
prefect config set PREFECT_UI_API_URL="http://$DEV_SERVER:$DEV_SERVER_PORT/api"

runServer() {
    echo "Starting prefect server"
    prefect server start
}

runDatabase() {
    echo "Starting Postgres database for IMAP dev"
    docker run --rm --name postgres_imap_dev -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DATABASE=imap -p 5432:5432 postgres:17-alpine
}

runWiremock() {
    echo "TODO! Start wiremock"
    #java -jar wiremock-standalone-2.27.2.jar --port 8080
}

deployToServer() {
    # let the server startup
    sleep 20

    echo "Deploying to server"
    prefect work-pool create default-pool --type process --overwrite

    PREFECT_LOGGING_ROOT_LEVEL="INFO" \
        PREFECT_LOGGING_EXTRA_LOGGERS="imap_mag,imap_db,mag_toolkit,prefect_server" \
        PREFECT_LOGGING_LEVEL=DEBUG \
        PREFECT_SERVER_LOGGING_LEVEL=${PREFECT_SERVER_LOGGING_LEVEL} \
        PREFECT_INTERNAL_LOGGING_LEVEL=${PREFECT_SERVER_LOGGING_LEVEL} \
            python -c 'import prefect_server.workflow; prefect_server.workflow.deploy_flows(local_debug=True)'

    echo "Deployment complete"
}

# ensure CTRL_C kills all the processes
(trap 'kill 0' SIGINT; runServer & runDatabase & runWiremock & deployToServer & wait)
