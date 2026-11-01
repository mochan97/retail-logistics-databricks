
"""Generate reproducible synthetic retail logistics shipments."""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


FIELDNAMES = [
    "shipment_id",
    "distribution_center",
    "store_id",
    "planned_delivery",
    "actual_delivery",
    "ordered_units",
    "delivered_units",
]


def generate_shipments(
    rows: int,
    seed: int,
    output: Path,
) -> None:
    if rows <= 0:
        raise ValueError("rows must be a positive integer")

    rng = random.Random(seed)
    start_date = date(2026, 7, 1)

    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()

        for i in range(1, rows + 1):
            # 100 stores served by three distribution centers
            store_number = rng.randint(1, 100)
            dc_number = (store_number - 1) % 3 + 1

            planned = start_date + timedelta(
                days=rng.randint(0, 90)
            )

            # Approx. 80% on-time shipments
            delay = rng.choices(
                [0, 1, 2, 3],
                weights=[80, 12, 6, 2],
                k=1,
            )[0]

            actual = planned + timedelta(days=delay)
            ordered = rng.randint(20, 500)

            fulfillment = rng.choices(
                ["COMPLETE", "UNDER", "OVER"],
                weights=[90, 8, 2],
                k=1,
            )[0]

            if fulfillment == "UNDER":
                delivered = ordered - rng.randint(
                    1, min(ordered, 20)
                )
            elif fulfillment == "OVER":
                delivered = ordered + rng.randint(1, 10)
            else:
                delivered = ordered

            writer.writerow({
                "shipment_id": f"S{i:06d}",
                "distribution_center": f"DC{dc_number:02d}",
                "store_id": f"STORE{store_number:03d}",
                "planned_delivery": planned.isoformat(),
                "actual_delivery": actual.isoformat(),
                "ordered_units": ordered,
                "delivered_units": delivered,
            })


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail shipments"
    )
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/generated/shipments_100k.csv"),
    )

    args = parser.parse_args()
    generate_shipments(args.rows, args.seed, args.output)

    print(f"Generated {args.rows:,} shipment records")
    print(f"File: {args.output}")


if __name__ == "__main__":
    main()
