#!/bin/bash
set -e

CLI_TOOL="imap_mag"
TOOL_PYTHON_VERSION="${TOOL_PYTHON_VERSION:-python3.12}"
TOOL_PACKAGE="${TOOL_PACKAGE:-$CLI_TOOL-*.tar.gz}"

if [ ! -f dist/$TOOL_PYTHON_VERSION/$TOOL_PACKAGE ]
then
    echo "Cannot find tar in dist/$TOOL_PYTHON_VERSION. Run build-all.sh?"
    exit 1
fi

# install using pipx for nice clean isolation
python -m pip install --user pipx

# use the tar file in the dist folder (assumes you already ran build-all.sh) to install the TOOL CLI as a global tool
pipx install --python $TOOL_PYTHON_VERSION dist/$TOOL_PYTHON_VERSION/$TOOL_PACKAGE

# (Optional) Check it worked?
# $CLI_TOOL --help

# (Optional) uninstall
# pipx uninstall $CLI_TOOL
