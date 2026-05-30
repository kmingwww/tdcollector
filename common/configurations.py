import os

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": { "format": "%(asctime)s [%(levelname)s] %(message)s" }
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        "root": { "level": "INFO", "handlers": ["stdout"] }
    }
}

def get_default_spreadsheet_id():
    return os.getenv("SPREADSHEET_ID", "1D1jB-6fH1a9UXXH8elbK77CVUM-wEYKD8dhNQNxJw9s")

def get_default_range_name():
    return os.getenv("RANGE_NAME", "COPYORDER_CONT!A:Z")

def get_default_credential_file():
    return os.getenv("SERVICE_ACCOUNT_FILE", "service_account_credential.json")
