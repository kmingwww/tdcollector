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
