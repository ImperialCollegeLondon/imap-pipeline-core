#!/bin/bash
set -e

# Example:
# ./run-docker.sh
# ./run-docker.sh -i   // for interactive debugging
# ./run-docker.sh -i -e VARIABLE=some_value  // for interactive debugging with VARIABLE env var set

IMAGE_NAME="${IMAGE_NAME:-imap-pipeline-core/imap-mag}"

echo "Running $IMAGE_NAME with dev.env file"

# check if the argument "DEBUG" or "-i" is passed
if [ "$1" == "debug" ] || [ "$1" == "DEBUG" ] || [ "$1" == "-i" ]; then
    echo "Overriding entrypoint to be an interactive bash shell for debugging"
    docker run --rm -it \
      --entrypoint /bin/bash \
      --env-file dev.env \
      -v /mnt/imap-data:/data \
      $IMAGE_NAME
elif [ -z "$1" ]; then # no args passed
    docker run --rm -it \
  --env-file dev.env \
  -v /mnt/imap-data:/data \
  $IMAGE_NAME
else
    echo "Extra arguments: $@"
    docker run --rm -it \
      --env-file dev.env \
      -v /mnt/imap-data:/data \
      $@ $IMAGE_NAME
fi
