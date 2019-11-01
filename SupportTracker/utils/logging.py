import logging

_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
_MESSAGE_FORMAT = "[{levelname}] [{asctime}] {message}"

# Module Level Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format=_MESSAGE_FORMAT,
    style="{",
    datefmt=_TIME_FORMAT
)
log = logging.getLogger(__name__)


def _log_info(msg: str):
    """Log an info message using the package logger

    :param msg: The message to log
    """
    log.info(msg)
