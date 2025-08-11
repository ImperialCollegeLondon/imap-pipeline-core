
# This was based off the default devcontainer for python and customised to support multiple python versions
# see https://github.com/devcontainers/images/blob/main/src/python/.devcontainer/Dockerfile

## we start with the version we want as the system version
FROM mcr.microsoft.com/devcontainers/python:3.12

# And then use pyenv to install all the other versions as the vscode user
ARG PYTHON_VERSIONS="3.10 3.11 3.12 3.13"
USER vscode
ENV PYENV_ROOT="/home/vscode/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
RUN curl https://pyenv.run | bash
RUN $PYENV_ROOT/bin/pyenv install -v ${PYTHON_VERSIONS}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client
