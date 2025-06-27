ARG PYTHON_VERSION="3.12"

FROM python:${PYTHON_VERSION}-slim AS compile-image

ARG IMAP_USERNAME="imap_pipeline"
ARG IMAP_USERID=5679

ARG TOOL_PACKAGE="imap_mag-*.tar.gz"
ARG PYTHON_VERSION
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install the postgres client and any other compile time dependencies needed to build our app
RUN apt-get update && apt-get install -y libpq-dev gcc git

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u ${IMAP_USERID} --disabled-password --gecos "" ${IMAP_USERNAME}

USER ${IMAP_USERNAME}

# first restore the python dependencies in a single cache layer - should get faster builds as this changes rarely
COPY dist/python${PYTHON_VERSION}/requirements.txt .
RUN python3 -m pip install --user --no-deps -r requirements.txt

# now install the imap-mag package as the user
COPY dist/python${PYTHON_VERSION}/${TOOL_PACKAGE} python${PYTHON_VERSION}/
RUN python3 -m pip install --user python${PYTHON_VERSION}/${TOOL_PACKAGE}

# create the runtime image and copy in the installed packed from compile-image
FROM python:${PYTHON_VERSION}-slim AS runtime-image

ARG IMAP_USERNAME="imap_pipeline"
ARG IMAP_USERID=5679

COPY --from=compile-image /home/${IMAP_USERNAME}/.local /home/${IMAP_USERNAME}/.local
COPY dist/docker /app

RUN adduser -u ${IMAP_USERID} --disabled-password --gecos "" ${IMAP_USERNAME} && \
    chown -R ${IMAP_USERNAME} /app && \
    mkdir -p /data && \
    chown -R ${IMAP_USERNAME} /data && \
    chown -R ${IMAP_USERNAME} /home/${IMAP_USERNAME}

WORKDIR /app

USER ${IMAP_USERNAME}
ENV PATH="$PATH:/home/${IMAP_USERNAME}/.local/bin"
ENV PREFECT_HOME=/app/.prefect
ENV PREFECT_PROFILES_PATH=/app/.prefect/profiles.toml

#include the prefect workflows
COPY src/ /app/

# Now the imap-mag CLI is available on the path and
# the 3.12 python package for imap-mag is intalled at /home/${IMAP_USERNAME}/.local/bin
