#!/bin/bash
#set -e

# create a version of the CLI tool that is frozen and packaged so needs no dependencies

# setup the dist dir if not already there, and clear it
mkdir -p dist

docker run \
  --volume "$(pwd):/src/imap_mag" \
   batonogov/pyinstaller-linux:latest \
  "rm -rf dist/pyinstaller && \
  python3 -m pip install poetry && \
  python3 -m poetry self add poetry-pyinstaller-plugin && \
  python3 -m poetry install && \
  python3 -m poetry build && \
  ./dist/pyinstaller/manylinux_2_36_x86_64/imap-mag hello world"


