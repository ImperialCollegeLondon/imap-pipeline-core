#!/bin/bash
set -e

# compile and test the app for the current python version

# what version is this?
poetry run python --version

# restore dependencies & create an venv if needed
echo "Restoring dependencies..."
poetry install --all-groups --all-extras

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
export PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW=ignore

if [ "$1" != "--skip-tests" ]; then
    args=(
        run pytest
        # distribute tests across 3 processes aggressively
        # See https://pytest-xdist.readthedocs.io/en/latest/distribution.html
        -n auto --dist loadscope --maxprocesses=3
        # Show test name, not all the log messages, and colorize output
        -vvv --log-disable=root --color=yes
        # coverage parameters
        --cov-config=.coveragerc --cov=src --cov-append --cov-report=xml --cov-report term-missing --cov-report=html --cov-fail-under=90
        --junitxml=test-results.xml # CI readable report
        --durations 20  # print top 20 slow tests
        tests # folder name of tests
    )
    poetry "${args[@]}"


else
    echo "Skipping tests"
fi

