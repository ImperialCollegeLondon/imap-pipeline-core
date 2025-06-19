#!/bin/bash
set -e

CLI_TOOL="imap_mag"
TOOL_PYTHON_VERSION="${TOOL_PYTHON_VERSION:-python3.12}"
TOOL_PACKAGE="${TOOL_PACKAGE:-$CLI_TOOL-*.tar.gz}"
IMAGE_NAME="${IMAGE_NAME:-ghcr.io/imperialcollegelondon/imap-pipeline-core:local-dev}"

if [ ! -f dist/$TOOL_PYTHON_VERSION/$TOOL_PACKAGE ]
then
    echo "Cannot find tar in dist/$TOOL_PYTHON_VERSION. Running pack.sh"
    ./pack.sh
fi

# compile imap-mag into a docker container
#docker build --progress=plain -f deploy/Dockerfile -t $IMAGE_NAME .

 if [ "$1" == "--local" ]
   then
    docker build --build-arg USERID=$UID -f deploy/MATLAB-Dockerfile -t $IMAGE_NAME .
 else
    docker build -f deploy/MATLAB-Dockerfile -t $IMAGE_NAME .
 fi

# Check the command works!
docker run \
  --entrypoint /bin/sh $IMAGE_NAME\
  -c "imap-mag hello world"
