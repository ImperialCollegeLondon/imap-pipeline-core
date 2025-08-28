#!/bin/bash
set -e

# compile and test the app for the current python version

# what version is this?
poetry run python --version

# restore dependencies & create an venv if needed
echo "Restoring dependencies..."
poetry install -q

# load the current python virtual environment - assumes you have already probably run "poetry shell" or are calling from build-python-versions.sh
if [ -d ".venv" ]; then
    # load the python virtual environment
    echo "Loading virtual env in .venv"
    source .venv/bin/activate
fi

# tidy up fomatting and check syntax
poetry run ruff check --fix

# Check the CLI actually runs as a basic CLI app
if poetry run imap-mag hello world | grep -q 'Hello world'; then
  echo "BUILD PASSED"
else
    echo "BUILD FAILED"
    exit 1
fi

#slow on GH Actions
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS="100"

if [ "$1" != "--skip-tests" ]; then
    args=(
        run pytest
        # distribute tests across 4 processes aggressively
        # See https://pytest-xdist.readthedocs.io/en/latest/distribution.html
        -n auto --dist worksteal --maxprocesses=4
        # coverage parameters
        --cov-config=.coveragerc --cov=src --cov-append --cov-report=xml --cov-report term-missing --cov-report=html
        --junitxml=test-results.xml # CI readable report
        --durations 10  # print top 10 slow tests
        tests # folder name of tests
    )
    poetry "${args[@]}"


else
    echo "Skipping tests"
fi

