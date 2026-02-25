import logging
import os
import sys
import traceback


def log_error(e):
    with open("error.log", "a", encoding="utf-8") as f:
        f.write(str(e) + "\n")
        traceback.print_exc(file=f)


def configure_logging(logger_name: str, log_file: str = "logs/redditstoriesgen.log") -> logging.Logger:
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[stream_handler, file_handler],
        force=True,
    )
    return logging.getLogger(logger_name)
