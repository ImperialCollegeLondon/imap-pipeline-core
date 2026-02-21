class PREFECT_CONSTANTS:
    DEFAULT_LOGGERS = "imap_mag,imap_db,mag_toolkit,prefect_server,imap_data_access,ialirt_data_access,crump"
    DEFAULT_WORKPOOL = "default-pool"

    PREFECT_TAG = "NASA-IMAP"

    SKIPPED_STATE_NAME = "Skipped"

    DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME = "imap-sharepoint"

    IMAP_DATASTORE_BLOCK_NAME = "imap-datastore"
    IMAP_WEBHOOK_BLOCK_NAME = "imap-teams-notification-webhook"

    IMAP_DATABASE_BLOCK_NAME = "imap-database"

    class EVENT:
        FLOW_RUN_COMPLETED = "prefect.flow-run.Completed"
        IALIRT_UPDATED = "imap_mag.ialirt.updated"

    class POLL_IALIRT:
        IALIRT_AUTH_CODE_SECRET_NAME = "ialirt-auth-code"
        IALIRT_QUICKLOOK_SHAREPOINT_URL = "https://imperiallondon.sharepoint.com/:i:/r/sites/IMAPFlightdata-PH/Shared%20Documents/Flight%20Data/quicklook/ialirt/latest.png"

    class POLL_HK:
        WEBPODA_AUTH_CODE_SECRET_NAME = "webpoda-auth-code"

    class POLL_SCIENCE:
        SDC_AUTH_CODE_SECRET_NAME = "sdc-auth-code"

    class POLL_WEBTCAD_LATIS:
        WEBPODA_AUTH_CODE_SECRET_NAME = "webpoda-auth-code"

    class ENV_VAR_NAMES:
        DATA_STORE_OVERRIDE = "MAG_DATA_STORE"

        POLL_IALIRT_CRON = "IMAP_CRON_POLL_IALIRT"
        CHECK_IALIRT_CRON = "IMAP_CRON_CHECK_IALIRT"
        POLL_HK_CRON = "IMAP_CRON_POLL_HK"
        POLL_L1C_NORM_CRON = "IMAP_CRON_POLL_L1C_NORM"
        POLL_L1B_BURST_CRON = "IMAP_CRON_POLL_L1B_BURST"
        POLL_L2_CRON = "IMAP_CRON_POLL_L2"
        POLL_L1D_CRON = "IMAP_CRON_POLL_L1D"
        POLL_SPICE_CRON = "IMAP_CRON_POLL_SPICE"
        POLL_WEBTCAD_LATIS_CRON = "IMAP_CRON_POLL_WEBTCAD_LATIS"
        IMAP_CRON_SHAREPOINT_UPLOAD = "IMAP_CRON_SHAREPOINT_UPLOAD"
        IMAP_CRON_POSTGRES_UPLOAD = "IMAP_CRON_POSTGRES_UPLOAD"
        IMAP_CRON_DATASTORE_CLEANUP = "IMAP_CRON_DATASTORE_CLEANUP"

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
        POLL_SPICE = "poll-spice"
        POLL_WEBTCAD_LATIS = "poll-webtcad-latis"
        CALIBRATE = "calibrate"
        APPLY_CALIBRATION = "apply-calibration"
        CALIBRATE_AND_APPLY = "calibrate-and-apply"
        GRADIOMETRY = "gradiometry"
        PUBLISH = "publish"
        CHECK_IALIRT = "check-ialirt"
        QUICKLOOK_IALIRT = "quicklook-ialirt"
        SHAREPOINT_UPLOAD = "sharepoint-upload"
        POSTGRES_UPLOAD = "postgres-upload"
        DATASTORE_CLEANUP = "datastore-cleanup"

    class DEPLOYMENT_NAMES:
        POLL_IALIRT = "poll_ialirt"
        POLL_HK = "poll_hk"
        POLL_SCIENCE = "poll_science"
        POLL_L1C_NORM = "poll_l1c_norm_science"
        POLL_L1B_BURST = "poll_l1b_burst_science"
        POLL_L2 = "poll_l2_science"
        POLL_L1D = "poll_l1d_science"
        POLL_SPICE = "poll_spice"
        POLL_WEBTCAD_LATIS = "poll_webtcad_latis"
        PUBLISH = "publish"
        CHECK_IALIRT = "check_ialirt"
        QUICKLOOK_IALIRT = "quicklook_ialirt"
        SHAREPOINT_UPLOAD = "sharepoint_upload"
        POSTGRES_UPLOAD = "postgres_upload"
        DATASTORE_CLEANUP = "datastore_cleanup"
