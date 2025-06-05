/bin/bash pack.sh
/bin/bash build-docker.sh

docker run -it --rm --network mag-lab-data-platform -e PREFECT_API_URL=http://prefect:4200/api -e IMAP_IMAGE_TAG=local-dev -e IMAP_VOLUMES=/mnt/imap-data/dev:/data --entrypoint /bin/bash ghcr.io/imperialcollegelondon/imap-pipeline-core:local-dev -c "python -c 'import prefect_server.workflow; prefect_server.workflow.deploy_flows()'" --env-file dev.env
