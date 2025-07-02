#!/bin/bash
set -e

# Pack the python code into a distributable format tag.gz & .whl

# if the folder ".venv" exists
if [ -d ".venv" ]; then
    #load the python virtual environment
    source .venv/bin/activate
fi

# you can set version with command "poetry version 1.2.3 before this, otherwise we just take it from the pyproject.toml file"
PYTHON_VERSION="$(python3 -c 'import sys; print (f"{sys.version_info.major}.{sys.version_info.minor}")')"
OUTPUT_DIST_FOLDER=dist/python$PYTHON_VERSION
echo "Packing version $(poetry version --short) for $(python3 --version) into $OUTPUT_DIST_FOLDER"
poetry lock
poetry build

# output a requierments.txt file used by docker during the build
poetry self add poetry-plugin-export
poetry export --without-hashes --format=requirements.txt > dist/requirements.txt

# move the files into a folder with the python version
mkdir -p dist/python$PYTHON_VERSION
find dist/ -maxdepth 1 -type f -name '*' -exec mv {} dist/python$PYTHON_VERSION \;

# setup a docker folder that will be copied into the docker output at /app
dockerFolder=dist/docker
mkdir -p $dockerFolder
cp -r resource $dockerFolder
cp *.yaml $dockerFolder
