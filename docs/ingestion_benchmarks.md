# CSV vs. Parquet — Ingestion Benchmark

## Objective

Evaluate CSV and Parquet ingestion into Databricks Delta tables using 100,000 synthetic retail shipment records.

## Environment

- Platform: Databricks Free Edition
- Processing engine: Apache Spark
- Source storage: Unity Catalog Volume
- Target storage: Delta Lake
- Parquet generation: Polars with Zstandard compression
- Processing strategy: Full refresh

## Results

| Metric | CSV | Parquet |
|---|---:|---:|
| Records | 100,000 | 100,000 |
| Source file size | 5.02 MiB | 0.54 MiB |
| Initial ingestion duration | 7.43 s | 4.05 s |

**Initial findings:**

- Parquet reduced source file size by 89.3%.
- Parquet ingestion showed approximately 45.5% lower elapsed time in the initial run.
- Both pipelines write into separate Delta tables.

## Limitations

The benchmark currently contains one measured run per format. Results may be affected by serverless initialization, caching, execution variability, and Delta Lake write overhead.

The figures represent the observed ingestion workflow, not isolated CSV or Parquet decoding performance.

## Next Steps

- Validate row-level equivalence.
- Repeat measurements under comparable conditions.
- Evaluate larger datasets if resources permit.
- Investigate incremental file ingestion using Databricks Auto Loader.