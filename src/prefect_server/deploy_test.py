from prefect.docker import DockerImage

from prefect_server.workflow import apply_flow, calibrate_flow

if __name__ == "__main__":
    docker_volumes = [
        "/home/mhairifin/DataPipeline/imap-pipeline-core/local-folder/data_store:/data"
    ]
    env_variables = dict(
        PREFECT_LOGGING_EXTRA_LOGGERS="imap_mag,imap_db,mag_toolkit",
        MLM_LICENSE_FILE="27004@matlab.cc.ic.ac.uk",
        PREFECT_API_URL="http://host.docker.internal:4200/api",
    )
    shared_job_variables = dict(
        env=env_variables,
        volumes=docker_volumes,
        networks=["host"],
        image_pull_policy="IfNotPresent",
    )
    calibrate_flow.deploy(
        name="calibrate",
        job_variables=shared_job_variables,
        work_pool_name="docker-pool",
        image=DockerImage(
            name="ghcr.io/imperialcollegelondon/imap-pipeline-core", tag="local-dev"
        ),
        push=False,
        build=False,
    )
    apply_flow.deploy(
        name="apply",
        job_variables=shared_job_variables,
        work_pool_name="docker-pool",
        image=DockerImage(
            name="ghcr.io/imperialcollegelondon/imap-pipeline-core", tag="local-dev"
        ),
        push=False,
        build=False,
    )
