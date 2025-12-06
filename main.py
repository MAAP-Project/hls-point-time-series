"""Extract a values from an HLS time series for a set of points in a MGRS tile"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import odc.stac
import pandas as pd
import rasterio
import rioxarray  # noqa
from maap.maap import MAAP
from odc.stac import ParsedItem
from pystac import Asset, Catalog, CatalogType, Item
from rasterio.session import AWSSession
from rasterio.warp import transform_bounds
from rustac import DuckdbClient
from shapely.geometry import box, mapping

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BBox = Tuple[float, float, float, float]

GDAL_CONFIG = {
    "CPL_TMPDIR": "/tmp",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": "TIF",
    "GDAL_CACHEMAX": "75%",
    "GDAL_INGESTED_BYTES_AT_OPEN": "32768",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_VERSION": "2",
    "PYTHONWARNINGS": "ignore",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "536870912",
    "GDAL_NUM_THREADS": "ALL_CPUS",
    # "CPL_DEBUG": "ON" if debug else "OFF",
    # "CPL_CURL_VERBOSE": "YES" if debug else "NO",
}

HLS_COLLECTIONS = ["HLSL30_2.0", "HLSS30_2.0"]
HLS_STAC_GEOPARQUET_HREF = "s3://nasa-maap-data-store/file-staging/nasa-map/hls-stac-geoparquet-archive/v2/{collection}/**/*.parquet"

URL_PREFIX = "https://data.lpdaac.earthdatacloud.nasa.gov/"
DTYPE = "int16"
FMASK_DTYPE = "uint8"
NODATA = -9999
FMASK_NODATA = 255
HLS_ODC_STAC_CONFIG = {
    "HLSL30_2.0": {
        "assets": {
            "*": {
                "nodata": NODATA,
                "data_type": DTYPE,
            },
            "Fmask": {
                "nodata": FMASK_NODATA,
                "data_type": FMASK_DTYPE,
            },
        },
        "aliases": {
            "coastal_aerosol": "B01",
            "blue": "B02",
            "green": "B03",
            "red": "B04",
            "nir_narrow": "B05",
            "swir_1": "B06",
            "swir_2": "B07",
            "cirrus": "B09",
            "thermal_infrared_1": "B10",
            "thermal": "B11",
        },
    },
    "HLSS30_2.0": {
        "assets": {
            "*": {
                "nodata": NODATA,
                "data_type": DTYPE,
            },
            "Fmask": {
                "nodata": FMASK_NODATA,
                "data_type": FMASK_DTYPE,
            },
        },
        "aliases": {
            "coastal_aerosol": "B01",
            "blue": "B02",
            "green": "B03",
            "red": "B04",
            "red_edge_1": "B05",
            "red_edge_2": "B06",
            "red_edge_3": "B07",
            "nir_broad": "B08",
            "nir_narrow": "B8A",
            "water_vapor": "B09",
            "cirrus": "B10",
            "swir_1": "B11",
            "swir_2": "B12",
        },
    },
}

# these are the ones that we are going to use
DEFAULT_BANDS = [
    "red",
    "green",
    "blue",
    "nir_narrow",
    "swir_1",
    "swir_2",
    "Fmask",
]
DEFAULT_RESOLUTION = 30


def parse_datetime_utc(dt_string: str) -> datetime:
    """
    Parse a datetime string and ensure it has UTC timezone.
    If no timezone is specified, assume UTC.

    Args:
        dt_string: ISO format datetime string (e.g., '2024-01-01T00:00:00' or '2024-01-01T00:00:00Z')

    Returns:
        datetime object with UTC timezone
    """
    dt = datetime.fromisoformat(dt_string.replace("Z", "+00:00"))

    # If the datetime is naive (no timezone), assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def group_by_sensor_and_date(
    item: Item,
    parsed: ParsedItem,
    idx: int,
) -> str:
    id_split = item.id.split(".")
    sensor = id_split[1]
    day = id_split[3][:7]

    return f"{sensor}_{day}"


def get_stac_items(
    mgrs_tile: str, start_datetime: datetime, end_datetime: datetime
) -> list[Item]:
    logger.info("querying HLS archive")
    client = DuckdbClient(use_hive_partitioning=True)
    client.execute(
        """
        CREATE OR REPLACE SECRET secret (
             TYPE S3,
             PROVIDER CREDENTIAL_CHAIN
        );
        """
    )

    items = []
    for collection in HLS_COLLECTIONS:
        items.extend(
            client.search(
                href=HLS_STAC_GEOPARQUET_HREF.format(collection=collection),
                datetime="/".join(
                    dt.isoformat() for dt in [start_datetime, end_datetime]
                ),
                filter={
                    "op": "and",
                    "args": [
                        {
                            "op": "like",
                            "args": [{"property": "id"}, f"%.T{mgrs_tile}.%"],
                        },
                        {
                            "op": "between",
                            "args": [
                                {"property": "year"},
                                start_datetime.year,
                                end_datetime.year,
                            ],
                        },
                    ],
                },
            )
        )

    logger.info(f"found {len(items)} items")

    return [Item.from_dict(item) for item in items]


def run(
    mgrs_tile: str,
    points_href: str,
    start_datetime: datetime,
    end_datetime: datetime,
    output_dir: Path,
    id_col: str | None = None,
    bands: list[str] = DEFAULT_BANDS,
    direct_bucket_access: bool = False,
    batch_size: int = 100,
):
    pts = gpd.read_file(points_href)

    if id_col:
        pts = pts.set_index(id_col)

    items = get_stac_items(
        mgrs_tile=mgrs_tile,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    if not items:
        logger.info(f"no STAC items found for tile id {mgrs_tile}")
        return

    rasterio_env = {}
    if direct_bucket_access:
        maap = MAAP(maap_host="api.maap-project.org")
        creds = maap.aws.earthdata_s3_credentials(
            "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
        )
        odc.stac.configure_rio(
            cloud_defaults=True,
            aws={
                "aws_access_key_id": creds["accessKeyId"],
                "aws_secret_access_key": creds["secretAccessKey"],
                "aws_session_token": creds["sessionToken"],
                "region_name": "us-west-2",
            },
        )
        rasterio_env["session"] = AWSSession(
            **{
                "aws_access_key_id": creds["accessKeyId"],
                "aws_secret_access_key": creds["secretAccessKey"],
                "aws_session_token": creds["sessionToken"],
                "region_name": "us-west-2",
            }
        )
        for item in items:
            for asset in item.assets.values():
                if asset.href.startswith(URL_PREFIX):
                    asset.href = asset.href.replace(URL_PREFIX, "s3://")

    logger.info("checking proj metadata")
    fixed_count = 0
    with rasterio.Env(**rasterio_env):
        for item in items:
            if (not item.ext.proj.shape) and (not item.ext.proj.transform):
                fixed_count += 1
                with rasterio.open(item.assets["Fmask"].href) as src:
                    item.ext.proj.shape = src.shape
                    item.ext.proj.transform = list(src.transform)

    logger.info(f"fixed proj metadata for {fixed_count} items")

    # get time dimension size
    logger.info(f"processing {len(items)} items in batches of {batch_size}")

    # process time dimension in batches with retry logic
    batch_dfs = []
    max_retries = 3
    retry_delay = 5  # seconds

    for batch_start in range(0, len(items), batch_size):
        batch_end = min(batch_start + batch_size, len(items))
        logger.info(f"processing batch {batch_start}:{batch_end}")

        for attempt in range(max_retries):
            try:
                batch_items = items[batch_start:batch_end]
                logger.info(
                    f"loading {len(batch_items)} items into xarray via odc.stac"
                )
                stack = (
                    odc.stac.load(
                        batch_items,
                        stac_cfg=HLS_ODC_STAC_CONFIG,
                        bands=bands,
                        chunks={"x": 512, "y": 512},
                        groupby=group_by_sensor_and_date,
                    )
                    .assign_coords(item_id=[item.id for item in batch_items])
                    .sortby("time")
                )

                crs = batch_items[0].ext.proj.crs_string
                if not crs:
                    raise ValueError("could not parse crs_string from STAC items")

                stack_extent = box(*stack.rio.bounds())

                # identify points within the MGRS tile extent
                pts_clipped = gpd.clip(pts.to_crs(crs), mask=stack_extent)

                # get projected coordinates
                pts_clipped["x"] = pts_clipped.geometry.x
                pts_clipped["y"] = pts_clipped.geometry.y

                # get the indexer for point locations
                indexer = pts_clipped[["x", "y"]].to_xarray()
                # extract array values at point locations for this time batch
                sample_batch = stack.sel(
                    x=indexer.x, y=indexer.y, method="nearest", tolerance=20
                )

                # convert to dataframe
                sample_batch_df = sample_batch.to_dataframe()

                # identify non-NA rows
                int16_cols = sample_batch_df[bands].select_dtypes("int16").columns
                valid = (sample_batch_df[int16_cols] != NODATA).all(axis=1)

                # append valid rows to batch list
                batch_dfs.append(sample_batch_df[valid])
                logger.info(f"successfully processed batch {batch_start}:{batch_end}")
                break

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)  # exponential backoff
                    logger.warning(
                        f"Batch {batch_start}:{batch_end} attempt {attempt + 1}/{max_retries} failed with error: {e}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"All {max_retries} attempts failed for batch {batch_start}:{batch_end}. Last error: {e}"
                    )
                    raise

    # combine all batches
    logger.info("combining all batches into final dataframe")
    sample_df = pd.concat(batch_dfs, axis=0)

    output_parquet = f"{output_dir}/point_sample.parquet"
    sample_df.to_parquet(output_parquet, compression="zstd")

    catalog = Catalog(
        id="DPS",
        description="DPS",
        catalog_type=CatalogType.SELF_CONTAINED,
    )

    bbox = transform_bounds(crs, "epsg:4326", *stack.rio.bounds())

    item = Item(
        id="-".join(
            [
                mgrs_tile,
                start_datetime.strftime("%Y%m%d"),
                end_datetime.strftime("%Y%m%d"),
            ]
        ),
        bbox=bbox,
        geometry=mapping(box(*bbox)),
        datetime=end_datetime,
        properties={
            "start_datetime": start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_datetime": end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        assets={
            "parquet": Asset(
                href=output_parquet,
                title="HLS time series point sample",
                description="HLS time series point sample",
            )
        },
    )
    item.ext.add("proj")
    item.ext.proj.apply(
        code=crs, bbox=stack.rio.bounds(), geometry=mapping(box(*stack.rio.bounds()))
    )

    item.set_self_href(f"{output_dir}/item.json")

    # finalize catalog and save to the output directory
    catalog.add_item(item)
    item.make_asset_hrefs_relative()

    catalog.normalize_and_save(
        root_href=str(output_dir),
        catalog_type=CatalogType.SELF_CONTAINED,
    )


if __name__ == "__main__":
    parse = argparse.ArgumentParser(
        description="Queries the HLS STAC geoparquet archive and extracts the raster values for a set of points"
    )
    parse.add_argument(
        "--start_datetime",
        help="start datetime in ISO format (e.g., 2024-01-01T00:00:00Z)",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--end_datetime",
        help="end datetime in ISO format (e.g., 2024-12-31T23:59:59Z)",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--mgrs_tile",
        help="MGRS tile id, e.g. 15XYZ",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--points_href",
        help="href for spatial points file (must have crs defined)",
        required=True,
        type=str,
    )
    parse.add_argument(
        "--id_col",
        help="column name to use to identify points in output dataframe",
        required=False,
        type=str,
    )
    parse.add_argument(
        "--bands",
        help="bands to extract (can be specified multiple times). Default: red, green, blue, nir_narrow, swir_1, swir_2, Fmask",
        required=False,
        action="append",
        type=str,
    )
    parse.add_argument(
        "--output_dir", help="Directory in which to save output", required=True
    )
    parse.add_argument(
        "--direct_bucket_access",
        help="Use direct S3 bucket access instead of HTTP URLs",
        action="store_true",
        default=False,
    )
    parse.add_argument(
        "--batch_size",
        help="Number of time steps to process in each batch (default: 100)",
        type=int,
        default=100,
    )
    args = parse.parse_args()

    output_dir = Path(args.output_dir)
    start_datetime = parse_datetime_utc(args.start_datetime)
    end_datetime = parse_datetime_utc(args.end_datetime)

    logging.info(
        f"setting GDAL config environment variables:\n{json.dumps(GDAL_CONFIG, indent=2)}"
    )
    os.environ.update(GDAL_CONFIG)

    logging.info(
        f"running with mgrs_tile: {args.mgrs_tile}, start_datetime: {start_datetime}, end_datetime: {end_datetime}"
    )

    run(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        mgrs_tile=args.mgrs_tile,
        points_href=args.points_href,
        id_col=args.id_col,
        bands=args.bands if args.bands else DEFAULT_BANDS,
        output_dir=output_dir,
        direct_bucket_access=args.direct_bucket_access,
        batch_size=args.batch_size,
    )
