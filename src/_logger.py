import logging


class LevelFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno in [logging.INFO, logging.DEBUG]:
            # Format for INFO and DEBUG level
            self._style._fmt = '[%(levelname)s] %(message)s'
        elif record.levelno == logging.WARNING:
            # Format for WARNING level
            self._style._fmt = '[%(levelname)s][%(name)s] %(message)s'
        else:  # Default format (for ERROR, CRITICAL or others)
            self._style._fmt = '[%(levelname)s][%(name)s][Line %(lineno)d][def %(funcName)s()] %(message)s'
        return super().format(record)


def create_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()

    formatter = LevelFormatter('%(message)s')
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    return logger


__all__ = (
    "create_logger",
)
