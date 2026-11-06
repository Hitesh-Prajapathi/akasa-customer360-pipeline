"""Entry point for incremental runs: python run_stream.py [--once] [--config path]

Default: poll the incoming folder every `stream.poll_seconds`, Ctrl+C to stop.
--once: process the batch files that are there now, then exit.
Needs a prior batch run (python run_pipeline.py) so the warehouse table exists.
"""
import argparse
import logging
import time

from customer360.incremental import load_config, process_new_batches
from customer360.logger import setup_logging

log = logging.getLogger("run_stream")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Customer 360 incremental (stream) runner")
    parser.add_argument("--config", default="config/pipeline.json", help="path to the pipeline config")
    parser.add_argument("--once", action="store_true", help="process new batches once and exit")
    args = parser.parse_args()
    cfg = load_config(args.config)
    setup_logging(cfg["log_file"])

    if args.once:
        done = process_new_batches(cfg)
        log.info("Processed %d new batch file(s)", len(done))
    else:
        log.info("Watching %s every %ss (Ctrl+C to stop)", cfg["stream"]["incoming_dir"], cfg["stream"]["poll_seconds"])
        try:
            while True:
                process_new_batches(cfg)
                time.sleep(cfg["stream"]["poll_seconds"])
        except KeyboardInterrupt:
            log.info("Stopped")
