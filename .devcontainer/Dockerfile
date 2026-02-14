
# This was based off the default devcontainer for python and customised to support multiple python versions
# see https://github.com/devcontainers/images/blob/main/src/python/.devcontainer/Dockerfile

## we start with the version we want as the system version
FROM mcr.microsoft.com/devcontainers/python:3.14-bookworm

# install uv from the UV image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG PYTHON_VERSIONS="3.11 3.12 3.13 3.14"
USER vscode

# Install multiple Python versions using `uv` as the non-root user
RUN uv python install $PYTHON_VERSIONS --install-dir /home/vscode/.local/bin

RUN sudo apt-get update && sudo apt-get install --no-install-recommends --no-install-suggests -y \
    postgresql-client
RUN python3 -m pip install --user pipx && \
    python3 -m pipx ensurepath
RUN pipx ensurepath && \
    pipx install poetry

ENV PATH="/home/vscode/.local/bin:${PATH}"

