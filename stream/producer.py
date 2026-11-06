"""Simulated stream: write micro-batches of order updates as CSV files.

    python -m stream.producer --batches 5 --size 20

Each batch_NNNN.csv holds `size` updates for existing customers: more orders, a higher
lifetime value and a last_order_date after 2026-10-28. A few deliberately bad rows are
added to the first batches (negative LTV, blank ID, unknown ID) so DQ runs on the stream.
A fixed random seed makes the output identical on every run.
"""
import argparse
import csv
import json
import os
import random
from datetime import date, timedelta

HEADER = ["cust_id", "total_orders", "lifetime_value", "last_order_date"]
FIRST_DAY = date(2026, 10, 28)
# One bad row per batch for the first three batches: (cust_id, total_orders, ltv).
BAD_ROWS = [("C0007", "5", "-1200"), ("", "3", "4000"), ("C9999", "2", "1500")]


def _to_int(value: str) -> int:
    """Base values from orders.csv; blank or negative counts as 0 (C0310, C0420)."""
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def read_current(orders_csv: str) -> dict[str, list[int]]:
    """customer_id -> [total_orders, lifetime_value] from the original orders file."""
    with open(orders_csv, newline="") as f:
        return {r["cust_id"]: [_to_int(r["total_orders"]), _to_int(r["lifetime_value"])]
                for r in csv.DictReader(f) if r["cust_id"]}


def generate_batches(orders_csv: str, out_dir: str, batches: int, size: int, seed: int = 42) -> list[str]:
    """Write batch_0001.csv ... and return their paths. Same arguments -> same files."""
    rng = random.Random(seed)
    current = read_current(orders_csv)
    ids = sorted(current)
    os.makedirs(out_dir, exist_ok=True)
    paths = []
    for n in range(1, batches + 1):
        order_date = (FIRST_DAY + timedelta(days=n)).isoformat()
        rows = []
        for customer_id in rng.sample(ids, size):
            # Values only grow, also when a customer appears in several batches.
            current[customer_id][0] += rng.randint(1, 3)
            current[customer_id][1] += rng.randint(500, 20000)
            rows.append([customer_id, current[customer_id][0], current[customer_id][1], order_date])
        if n <= len(BAD_ROWS):
            rows.append([*BAD_ROWS[n - 1], order_date])
        path = os.path.join(out_dir, f"batch_{n:04d}.csv")
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(HEADER)
            writer.writerows(rows)
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Write simulated order-update micro-batches")
    parser.add_argument("--config", default="config/pipeline.json")
    parser.add_argument("--batches", type=int, default=5)
    parser.add_argument("--size", type=int, default=20, help="good rows per batch")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = json.load(f)
    orders_csv = os.path.join(cfg["input_dir"], cfg["stream"]["source"])
    for path in generate_batches(orders_csv, cfg["stream"]["incoming_dir"], args.batches, args.size, args.seed):
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
