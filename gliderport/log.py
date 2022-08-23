import logging
import sys

GLIDER_PORT_FORMAT = "%(levelname).1s %(asctime)s %(filename)s:%(lineno)d] %(message)s"

DATE_FORMAT = "%m-%d %H:%M:%S"


class NewLineFormatter(logging.Formatter):
    """Adds logging prefix to newlines to align multi-line messages."""

    def __init__(self, fmt, datefmt=None):
        logging.Formatter.__init__(self, fmt, datefmt)

    def format(self, record):
        """Format logging."""
        msg = logging.Formatter.format(self, record)
        if record.message != "":
            parts = msg.split(record.message)
            msg = msg.replace("\n", "\r\n" + parts[0])
        return msg


def init_logger(name: str):
    """Initialize logger."""
    h = logging.StreamHandler(sys.stdout)
    h.flush = sys.stdout.flush

    fmt = NewLineFormatter(GLIDER_PORT_FORMAT, datefmt=DATE_FORMAT)
    h.setFormatter(fmt)

    logger = logging.getLogger(name)
    logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger
