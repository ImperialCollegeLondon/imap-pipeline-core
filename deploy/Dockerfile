ARG PYTHON_VERSION="3.12"

FROM python:${PYTHON_VERSION}-slim AS compile-image

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
RUN adduser -u 5678 --disabled-password --gecos "" appuser

USER appuser

# first restore the python dependencies in a single cache layer - should get faster builds as this changes rarely
COPY dist/python${PYTHON_VERSION}/requirements.txt .
RUN python3 -m pip install --user -r requirements.txt

# now install the imap-mag package as the user
COPY dist/python${PYTHON_VERSION}/${TOOL_PACKAGE} python${PYTHON_VERSION}/
RUN python3 -m pip install --user python${PYTHON_VERSION}/${TOOL_PACKAGE}

# create the runtime image and copy in the installed packed from compile-image
FROM python:${PYTHON_VERSION}-slim AS runtime-image
COPY --from=compile-image /home/appuser/.local /home/appuser/.local
COPY dist/docker /app

RUN adduser -u 5678 --disabled-password --gecos "" appuser && \
    chown -R appuser /app && \
    mkdir -p /data && \
    chown -R appuser /data && \
    chmod +x /app/entrypoint.sh

WORKDIR /app
USER appuser

# Make sure scripts in .local are usable:
ENV PATH="$PATH:/home/appuser/.local/bin"

# Now the imap-mag CLI is available on the path and
# the 3.12 python package for imap-mag is intalled at /home/appuser/.local/bin

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["/app/entrypoint.sh"]

