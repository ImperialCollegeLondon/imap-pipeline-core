#!bash

# Start a development environment for the project
# launch a prefect server
# deploy the project to the server
#start any other services like wiremock

set -e

# VSCODE TASK WILL USE THIS MESSAGE TO KNOW WHEN TASK IS STARTING
echo "STARTING DEV SERVERS"
source ./defaults.env

if [ -f ".env" ]; then
    echo "Loading local .env overrides"
    source .env
else
    echo "No local .env file found, skipping"
fi

# if .venv is missing, run poetry install
if [ ! -d ".venv" ]; then
    echo ".venv not found, running 'poetry install'"
    poetry install -q
fi

# if prefect command is not available source the venv
if ! command -v prefect &> /dev/null
then
    echo "prefect could not be found, sourcing venv"
    source .venv/bin/activate
fi


echo "Configure prefect server to run at http://$DEV_SERVER:$DEV_SERVER_PORT"
prefect config set PREFECT_API_URL="http://$DEV_SERVER:$DEV_SERVER_PORT/api"
prefect config set PREFECT_UI_API_URL="http://$DEV_SERVER:$DEV_SERVER_PORT/api"

runServer() {
    echo "Starting prefect server"
    prefect server start
}

runDatabase() {

    echo "Starting Postgres database for IMAP dev"
    DB_CONTAINER=postgres_imap_dev
    # remove it if needed
    CONTAINER_ID=$(docker container ls --all --filter name=$DB_CONTAINER -q)
    if [ ! -z "$CONTAINER_ID" ]; then
        echo "Removing existing database container $DB_CONTAINER"
        docker container rm -f $DB_CONTAINER
    fi

    docker run --rm --name $DB_CONTAINER -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DATABASE=imap -p 5432:5432 postgres:17-alpine
}

runWiremock() {
    echo "TODO! Start wiremock"
    #java -jar wiremock-standalone-2.27.2.jar --port 8080
}

deployToServer() {


    echo "Waiting for server to start up. Polling http://$DEV_SERVER:$DEV_SERVER_PORT/api/health"

    # let the server startup
    until $(curl --output /dev/null --silent --fail http://$DEV_SERVER:$DEV_SERVER_PORT/api/health); do
        printf '.'
        sleep 5
    done

    # if the first arg is --no-deploy, skip deployment
    if [ "$SKIP_DEPLOY" == "--no-deploy" ]; then

        # since not doing the deployment we need to setup the database
        imap-db create-db
        imap-db upgrade-db

        # VSCODE TASK WILL USE THIS MESSAGE TO KNOW WHEN TO PROCEED
        echo "SERVERS HAVE STARTED - Skipping deployment to server"
        return
    fi

    echo "Deploying to server"
    prefect work-pool create default-pool --type process --overwrite

    python -c 'import prefect_server.workflow; prefect_server.workflow.deploy_flows(local_debug=True)'

    echo "Deployment complete"
}

SKIP_DEPLOY=""
if [ "$1" == "--no-deploy" ]; then
    SKIP_DEPLOY="--no-deploy"
fi

# ensure CTRL_C kills all the processes
(trap 'kill 0' SIGINT; runServer & runDatabase & runWiremock & deployToServer & wait)
