# helpers/LoggingConfig.py
import logging

def setup_logging():
    """
    Setup global logging for the project:
    - INFO level for your code
    - Suppress all logs from ib_async
    """
    # 1️⃣ Configure root logger for your app
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(levelname)s: %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # 2️⃣ Silence ib_async completely
    ib_logger = logging.getLogger("ib_async")
    ib_logger.addHandler(logging.NullHandler())  # discard messages
    ib_logger.propagate = False                  # don't bubble to root
    ib_logger.setLevel(logging.CRITICAL + 1)    # ignore all levels
