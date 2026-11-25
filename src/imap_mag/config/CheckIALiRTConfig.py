from imap_mag.config.CommandConfig import CommandConfig


class CheckIALiRTConfig(CommandConfig):
    # Link to a flow run in the platform app shell, put in anomaly notifications.
    # "{flow_run_id}" is replaced with the run's id.
    run_url_template: str = (
        "https://mag-pipeline.ph.ic.ac.uk/app/pipelines/runs/flow-run/{flow_run_id}"
    )
