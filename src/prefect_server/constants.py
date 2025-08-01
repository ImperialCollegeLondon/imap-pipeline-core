class PREFECT_CONSTANTS:
    DEFAULT_LOGGERS = "imap_mag,imap_db,mag_toolkit,prefect_server,imap_data_access"
    DEFAULT_WORKPOOL = "default-pool"

    PREFECT_TAG = "NASA-IMAP"

    SKIPPED_STATE_NAME = "Skipped"

    class POLL_HK:
        WEBPODA_AUTH_CODE_SECRET_NAME = "webpoda-auth-code"

    class POLL_SCIENCE:
        SDC_AUTH_CODE_SECRET_NAME = "sdc-auth-code"

    class ENV_VAR_NAMES:
        DATA_STORE_OVERRIDE = "MAG_DATA_STORE"

        IMAP_PIPELINE_CRON = "IMAP_CRON_HEALTHCHECK"
        POLL_HK_CRON = "IMAP_CRON_POLL_HK"
        POLL_L1C_NORM_CRON = "IMAP_CRON_POLL_L1C_NORM"
        POLL_L1B_BURST_CRON = "IMAP_CRON_POLL_L1B_BURST"
        POLL_L2_CRON = "IMAP_CRON_POLL_L2"

        SQLALCHEMY_URL = "SQLALCHEMY_URL"

        PREFECT_LOGGING_EXTRA_LOGGERS = "PREFECT_LOGGING_EXTRA_LOGGERS"

        MATLAB_LICENSE = "MLM_LICENSE_FILE"

    class QUEUES:
        HIGH_PRIORITY = "high-priority"
        DEFAULT = "default"
        LOW = "low"

    class FLOW_NAMES:
        POLL_HK = "poll-hk"
        POLL_SCIENCE = "poll-science"
        CALIBRATE = "calibrate"
        APPLY_CALIBRATION = "apply-calibration"
        CALIBRATE_AND_APPLY = "calibrate-and-apply"
        PUBLISH = "publish"

    class DEPLOYMENT_NAMES:
        POLL_HK = "poll_hk"
        POLL_L1C_NORM = "poll_l1c_norm_science"
        POLL_L1B_BURST = "poll_l1b_burst_science"
        POLL_L2 = "poll_l2_science"
        PUBLISH = "publish"
