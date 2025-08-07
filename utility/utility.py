import logging
import sys

def setup_logger(log_file_name):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console output
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)

    # Log file output
    fh = logging.FileHandler(log_file_name)
    fh.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger

def format_seconds(seconds):
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{int(h)}h {int(m)}m {int(s)}s"