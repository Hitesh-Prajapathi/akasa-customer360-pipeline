"""Logging setup: the same messages go to the console and to a log file."""
import logging
import os


def setup_logging(log_file: str, level: int = logging.INFO) -> None:
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    # force=True resets handlers so repeated runs in one process don't duplicate lines
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, mode="a")],
        force=True,
    )
