
"""Convert synthetic shipment CSV to typed Parquet with Polars."""

import argparse
from pathlib import Path

import polars as pl


def convert_to_parquet(
    source: Path,
    destination: Path,
) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"CSV not found: {source}")

    destination.parent.mkdir(parents=True, exist_ok=True)

    data = (
        pl.scan_csv(
            source,
            schema_overrides={
                "shipment_id": pl.String,
                "distribution_center": pl.String,
                "store_id": pl.String,
                "ordered_units": pl.Int64,
                "delivered_units": pl.Int64,
            },
        )
        .with_columns(
            pl.col("planned_delivery").str.strptime(
                pl.Date, "%Y-%m-%d", strict=True
            ),
            pl.col("actual_delivery").str.strptime(
                pl.Date, "%Y-%m-%d", strict=True
            ),
        )
    )

    data.sink_parquet(
        destination,
        compression="zstd",
    )

    csv_size = source.stat().st_size
    parquet_size = destination.stat().st_size

    reduction = 100 * (1 - parquet_size / csv_size)

    print(f"CSV:     {csv_size / 1024**2:.2f} MiB")
    print(f"Parquet: {parquet_size / 1024**2:.2f} MiB")
    print(f"Size reduction: {reduction:.1f}%")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert shipment CSV to Parquet"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/generated/shipments_100k.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/generated/shipments_100k.parquet"),
    )

    args = parser.parse_args()

    convert_to_parquet(args.input, args.output)


if __name__ == "__main__":
    main()
