
"""Generate synthetic shipment tracking events as JSON Lines."""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_events(batch: int, rows: int, output_dir: Path):
    if batch < 1 or rows < 1:
        raise ValueError("batch and rows must be positive")

    rng = random.Random(42 + batch)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"batch_{batch:03d}.json"

    statuses = [
        "PICKED_UP",
        "IN_TRANSIT",
        "AT_DISTRIBUTION_CENTER",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "DELAYED",
    ]

    start = datetime(
        2026, 10, 1, tzinfo=timezone.utc
    )

    with output_file.open("w", encoding="utf-8") as f:
        for i in range(rows):
            event_number = (batch - 1) * rows + i + 1

            event = {
                "event_id": f"EVT{event_number:08d}",
                "shipment_id": f"S{rng.randint(1, 100000):06d}",
                "event_timestamp": (
                    start + timedelta(minutes=event_number)
                ).isoformat(),
                "event_type": rng.choice(statuses),
                "distribution_center": f"DC{rng.randint(1, 3):02d}",
                "source_system": "synthetic_tracking_api",
            }

            f.write(json.dumps(event) + "\n")

    print(f"Created {output_file} with {rows:,} events")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/generated/tracking_events"),
    )

    args = parser.parse_args()

    generate_events(
        args.batch,
        args.rows,
        args.output_dir,
    )
