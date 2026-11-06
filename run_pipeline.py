"""Entry point: python run_pipeline.py [--config config/pipeline.json]"""
import argparse

from customer360.pipeline import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Customer 360 pipeline")
    parser.add_argument("--config", default="config/pipeline.json", help="path to the pipeline config")
    run(parser.parse_args().config)
