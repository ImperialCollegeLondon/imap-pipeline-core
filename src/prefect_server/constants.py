class PREFECT_CONSTANTS:
    DEFAULT_LOGGERS = "imap_mag,imap_db,mag_toolkit,prefect_server,imap_data_access,ialirt_data_access"
    DEFAULT_WORKPOOL = "default-pool"

    PREFECT_TAG = "NASA-IMAP"

    SKIPPED_STATE_NAME = "Skipped"

    SHAREPOINT_BLOCK_NAME = "imap-sharepoint"

    IMAP_DATASTORE_BLOCK_NAME = "imap-datastore"
    IMAP_WEBHOOK_NAME = "imap-teams-notification-webhook"

    class EVENT:
        FLOW_RUN_COMPLETED = "prefect.flow-run.Completed"
        IALIRT_UPDATED = "imap_mag.ialirt.updated"

    class POLL_IALIRT:
        IALIRT_DATABASE_WORKFLOW_NAME = "MAG_IALIRT"
        IALIRT_AUTH_CODE_SECRET_NAME = "ialirt-auth-code"
        IALIRT_QUICKLOOK_SHAREPOINT_URL = "https://imperiallondon.sharepoint.com/:i:/r/sites/IMAPFlightdata-PH/Shared%20Documents/Flight%20Data/quicklook/ialirt/latest.png"

    class POLL_HK:
        WEBPODA_AUTH_CODE_SECRET_NAME = "webpoda-auth-code"

    class POLL_SCIENCE:
        SDC_AUTH_CODE_SECRET_NAME = "sdc-auth-code"

    class ENV_VAR_NAMES:
        DATA_STORE_OVERRIDE = "MAG_DATA_STORE"

        IMAP_PIPELINE_CRON = "IMAP_CRON_HEALTHCHECK"
        POLL_IALIRT_CRON = "IMAP_CRON_POLL_IALIRT"
        POLL_HK_CRON = "IMAP_CRON_POLL_HK"
        POLL_L1C_NORM_CRON = "IMAP_CRON_POLL_L1C_NORM"
        POLL_L1B_BURST_CRON = "IMAP_CRON_POLL_L1B_BURST"
        POLL_L2_CRON = "IMAP_CRON_POLL_L2"
        IMAP_CRON_SHAREPOINT_UPLOAD = "IMAP_CRON_SHAREPOINT_UPLOAD"

        SQLALCHEMY_URL = "SQLALCHEMY_URL"

        PREFECT_LOGGING_EXTRA_LOGGERS = "PREFECT_LOGGING_EXTRA_LOGGERS"

        MATLAB_LICENSE = "MLM_LICENSE_FILE"

    class QUEUES:
        HIGH_PRIORITY = "high-priority"
        DEFAULT = "default"
        LOW = "low"

    class FLOW_NAMES:
        POLL_IALIRT = "poll-ialirt"
        POLL_HK = "poll-hk"
        POLL_SCIENCE = "poll-science"
        CALIBRATE = "calibrate"
        APPLY_CALIBRATION = "apply-calibration"
        CALIBRATE_AND_APPLY = "calibrate-and-apply"
        GRADIOMETRY = "gradiometry"
        PUBLISH = "publish"
        CHECK_IALIRT = "check-ialirt"
        QUICKLOOK_IALIRT = "quicklook-ialirt"
        SHAREPOINT_UPLOAD = "sharepoint-upload"

    class DEPLOYMENT_NAMES:
        POLL_IALIRT = "poll_ialirt"
        POLL_HK = "poll_hk"
        POLL_L1C_NORM = "poll_l1c_norm_science"
        POLL_L1B_BURST = "poll_l1b_burst_science"
        POLL_L2 = "poll_l2_science"
        PUBLISH = "publish"
        CHECK_IALIRT = "check_ialirt"
        QUICKLOOK_IALIRT = "quicklook_ialirt"
        SHAREPOINT_UPLOAD = "sharepoint_upload"
