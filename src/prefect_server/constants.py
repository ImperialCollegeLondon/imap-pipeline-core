class CONSTANTS:
    DEFAULT_LOGGERS = "imap_mag,imap_db,mag_toolkit"
    DEFAULT_WORKPOOL = "default-pool"

    PREFECT_TAG = "NASA-IMAP"

    SKIPPED_STATE_NAME = "Skipped"

    class POLL_HK:
        WEBPODA_AUTH_CODE_SECRET_NAME = "webpoda-auth-code"

    class ENV_VAR_NAMES:
        IMAP_PIPELINE_CRON = "IMAP_CRON_HEALTHCHECK"
        POLL_HK_CRON = "IMAP_CRON_POLL_HK"

    class QUEUES:
        HIGH_PRIORITY = "high-priority"
        DEFAULT = "default"
        LOW = "low"

    class FLOW_NAMES:
        POLL_HK = "poll-hk"

    class DEPLOYMENT_NAMES:
        POLL_HK = "poll_hk"
