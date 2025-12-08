# HLS Point Time Series Extraction

Extract HLS time series values for a set of spatial points within an MGRS tile using STAC Geoparquet

## Motivation

The CMR STAC API has imposed rate limits on the HLS collections. This algorithm queries the HLS STAC records directly from parquet files in S3 without any API rate limits, then extracts time series values at specific point locations.

## About

This DPS algorithm uses [`rustac`](https://github.com/stac-utils/rustac-py) to query an archive of HLS STAC records stored as STAC Geoparquet, then extracts raster values at point locations. The workflow:

1. Queries HLS STAC items for a given MGRS tile and time range
2. Clips input points to the MGRS tile extent
3. Extracts raster values at each point location using nearest neighbor sampling
4. Filters out rows with nodata values
5. Exports the result as a Parquet file with STAC metadata

By using `rustac` + parquet files there is no API between the requester and the actual data!

> [!WARNING]
> This archive of HLS STAC records is experimental and is ~1.5 months behind the current time. 
> See the [hls-stac-geoparquet-archive repo](https://github.com/MAAP-Project/hls-stac-geoparquet-archive) for details.

## Usage

### MAAP DPS

To run the algorithm via DPS, you can follow this example. Provide the S3 URI for an input spatial file using the `input_file` argument. This file will be read using `geopandas` then the records will be filtered down to the ones that intersect the HLS raster asset footprint for the selected MGRS tile.

```python
from maap.maap import MAAP

maap = MAAP(maap_host="api.maap-project.org")

jobs = []
for tile in ["14VLQ", "18WXS", "16WFB", "26WMC", "19VDL"]:
    job = maap.submitJob(
        algo_id="HLSPointTimeSeriesExtraction",
        version="v0.2",
        identifier="test-run",
        queue="maap-dps-worker-16gb",
        input_file="s3://maap-ops-workspace/shared/henrydevseed/hls-boreal-sample-points.gpkg",
        start_datetime="2013-01-01T00:00:00Z",
        end_datetime="2025-10-31T23:59:59Z",
        mgrs_tile=tile,
        id_col="sample.id",
        bands="red green blue swir_1 swir_2 nir_narrow Fmask",
    )
    jobs.append(job)

```

Each job will produce a single parquet file in the DPS output folder along with a STAC item. The parquet files for this set of jobs can be read with `duckdb` like this:
```sql
CREATE OR REPLACE SECRET secret (
     TYPE S3,
     PROVIDER CREDENTIAL_CHAIN
);

SELECT * from read_parquet('s3://maap-ops-workspace/<YOUR_USERNAME>/dps_output/HLSPointTimeSeriesExtraction/v0.2/test-run/**/*.parquet');
```

### Direct Python Invocation

**Basic usage with default bands:**
```bash
uv run main.py \
  --start_datetime "2013-04-01T00:00:00Z" \
  --end_datetime "2013-05-31T23:59:59Z" \
  --mgrs_tile "15UWP" \
  --points_href "test_data/points.geojson" \
  --id_col "point_id" \
  --output_dir "/tmp/output" \
  --batch_size 2 \
  --direct_bucket_access  # optional: use S3 URIs instead of HTTPS (must be running in us-west-2)
```

**Custom band selection:**
```bash
uv run main.py \
  --start_datetime "2024-01-01T00:00:00Z" \
  --end_datetime "2024-12-31T23:59:59Z" \
  --mgrs_tile "15UWP" \
  --points_href "test_data/points.geojson" \
  --bands red --bands green --bands blue --bands Fmask \
  --output_dir "./output"
```

### Shell Script Wrapper for DPS

**Basic usage (4 required arguments):**
```bash
./run.sh "2024-01-01T00:00:00Z" "2024-12-31T23:59:59Z" "15TYK" "/path/to/points.geojson"
```

**With optional id_col and custom bands (6 arguments):**
```bash
./run.sh "2024-01-01T00:00:00Z" "2024-12-31T23:59:59Z" "15TYK" "/path/to/points.geojson" "point_id" "red green blue Fmask"
```

The script automatically creates the `output` directory and handles the `input` directory structure expected by DPS.

### Parameters

- `--start_datetime`: Start datetime in ISO format (e.g., 2024-01-01T00:00:00Z)
- `--end_datetime`: End datetime in ISO format (e.g., 2024-12-31T23:59:59Z)
- `--mgrs_tile`: MGRS tile identifier (e.g., 15TYK)
- `--points_href`: Path or URL to a spatial points file (must have CRS defined). Supported formats include GeoJSON, GeoParquet, Shapefile, etc.
- `--id_col`: Optional column name to use as the index for points in the output dataframe
- `--bands`: Optional bands to extract (can be specified multiple times). Default: red, green, blue, nir_narrow, swir_1, swir_2, Fmask. In shell script, provide as space-separated string (e.g., "red green blue")
- `--output_dir`: Directory where output files will be saved (handled automatically by shell script)
- `--direct_bucket_access`: Optional flag to use S3 URIs instead of HTTPS URLs for faster data access (requires AWS credentials for us-west-2)

## Details

The partitioned parquet dataset is available in my shared folder in the `maap-ops-workspace` bucket. The MAAP ADE and DPS both will have permissions to read from the archive.

The algorithm groups HLS observations by sensor (L30/S30) and date, loads all spectral bands and the Fmask layer within the MGRS tile extent, and extracts values at each point location using nearest neighbor sampling (within 20m tolerance). The output includes a STAC catalog with a Parquet file containing the extracted time series values for all requested bands (default: red, green, blue, nir_narrow, swir_1, swir_2, and Fmask).

The algorithm includes retry logic with exponential backoff to handle intermittent failures during data access.
