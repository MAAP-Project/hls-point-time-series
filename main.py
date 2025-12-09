"""Extract a values from an HLS time series for a set of points in a MGRS tile"""

import argparse
import json
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any, Tuple

import geopandas as gpd
import pandas as pd
import rasterio
from maap.maap import MAAP
from pystac import Asset, Catalog, CatalogType, Item
from rasterio.session import AWSSession
from rasterio.warp import transform
from rustac import DuckdbClient
from shapely.geometry import box, mapping

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logging.getLogger("botocore").setLevel(logging.WARNING)
logger = logging.getLogger("HLSPointTimeSeries")

BBox = Tuple[float, float, float, float]

GDAL_CONFIG = {
    "CPL_TMPDIR": "/tmp",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": "TIF,GPKG",
    "GDAL_CACHEMAX": "512",
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

# LPCLOUD S3 CREDENTIAL REFRESH
CREDENTIAL_REFRESH_SECONDS = 50 * 60

HLS_COLLECTIONS = ["HLSL30_2.0", "HLSS30_2.0"]
HLS_STAC_GEOPARQUET_HREF = "s3://nasa-maap-data-store/file-staging/nasa-map/hls-stac-geoparquet-archive/v2/{collection}/**/*.parquet"

URL_PREFIX = "https://data.lpdaac.earthdatacloud.nasa.gov/"
DTYPE = "int16"
FMASK_DTYPE = "uint8"
NODATA = -9999
FMASK_NODATA = 255

BAND_MAPPING = {
    "HLSL30_2.0": {
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
        "Fmask": "Fmask",
    },
    "HLSS30_2.0": {
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
        "Fmask": "Fmask",
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


def get_s3_creds() -> dict[str, Any]:
    maap = MAAP(maap_host="api.maap-project.org")
    creds = maap.aws.earthdata_s3_credentials(
        "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
    )

    return {
        "aws_access_key_id": creds["accessKeyId"],
        "aws_secret_access_key": creds["secretAccessKey"],
        "aws_session_token": creds["sessionToken"],
        "region_name": "us-west-2",
    }


def fetch_single_asset(
    item_id: str,
    collection_id: str,
    band_name: str,
    asset_href: str,
    coords_4326: list[tuple[float, float]],
    rasterio_env: dict[str, Any],
) -> tuple[str, str, list[float | int | None]]:
    """
    Fetch values from a single asset for given coordinates.
    Reprojects coordinates from EPSG:4326 to match the raster's CRS.
    Returns (item_id, band_name, values).
    """
    try:
        with rasterio.Env(**rasterio_env):
            with rasterio.open(asset_href) as src:
                raster_crs = src.crs.to_string()
                xs_4326, ys_4326 = zip(*coords_4326)
                xs_proj, ys_proj = transform("EPSG:4326", raster_crs, xs_4326, ys_4326)
                coords_proj = list(zip(xs_proj, ys_proj))

                values = list(src.sample(coords_proj))

                return item_id, band_name, [v[0] for v in values]

    except Exception as e:
        logger.warning(f"Failed to read {band_name} for {item_id}: {e}")
        return item_id, band_name, [None] * len(coords_4326)


def run(
    mgrs_tile: str,
    points_href: str,
    start_datetime: datetime,
    end_datetime: datetime,
    output_dir: Path,
    id_col: str | None = None,
    bands: list[str] = DEFAULT_BANDS,
    direct_bucket_access: bool = False,
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

    item_bboxes = [item.bbox for item in items if item.bbox]
    stack_bbox = (
        min(xmin for (xmin, _, _, _) in item_bboxes),
        min(ymin for (_, ymin, _, _) in item_bboxes),
        max(xmax for (_, _, xmax, _) in item_bboxes),
        max(ymax for (_, _, _, ymax) in item_bboxes),
    )

    stack_extent = box(*stack_bbox)

    # identify points within the MGRS tile extent (in EPSG:4326)
    pts_clipped = gpd.clip(pts.to_crs("EPSG:4326"), mask=stack_extent)
    logger.info(f"extracting values for {pts_clipped.shape[0]} points")

    rasterio_env = {}
    cred_time = None
    if direct_bucket_access:
        cred_time = time.time()
        s3_creds = get_s3_creds()
        rasterio_env["session"] = AWSSession(**s3_creds)
        for item in items:
            for asset in item.assets.values():
                if asset.href.startswith(URL_PREFIX):
                    asset.href = asset.href.replace(URL_PREFIX, "s3://")

    # build all tasks upfront: (item, band, asset_href) tuples
    # keep coordinates in EPSG:4326 - they'll be reprojected per-raster
    coords_4326 = list(zip(pts_clipped.geometry.x, pts_clipped.geometry.y))
    tasks = []
    item_metadata = {}  # store item metadata for later reconstruction

    for item in items:
        item_metadata[item.id] = {
            "datetime": item.datetime,
            "collection_id": item.collection_id,
        }
        for band_name in bands:
            asset_key = BAND_MAPPING[item.collection_id][band_name]
            asset_href = item.assets[asset_key].href
            tasks.append((item.id, item.collection_id, band_name, asset_href))

    logger.info(f"queued {len(tasks)} asset read tasks across {len(items)} items")

    # process all tasks in parallel with retry logic
    max_retries = 3
    retry_delay = 5  # seconds
    max_workers = 16  # increase parallelism across all items

    def fetch_with_retry(
        task_info: tuple[str, str, str, str],
    ) -> tuple[str, str, list[float | int | None]]:
        item_id, collection_id, band_name, asset_href = task_info

        # check if credentials need refresh
        if cred_time:
            elapsed = time.time() - cred_time
            if elapsed > CREDENTIAL_REFRESH_SECONDS:
                logger.info("refreshing S3 credentials in worker")
                s3_creds = get_s3_creds()
                worker_env = rasterio_env.copy()
                worker_env["session"] = AWSSession(**s3_creds)
            else:
                worker_env = rasterio_env
        else:
            worker_env = rasterio_env

        for attempt in range(max_retries):
            try:
                return fetch_single_asset(
                    item_id,
                    collection_id,
                    band_name,
                    asset_href,
                    coords_4326,
                    worker_env,
                )
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)
                    logger.warning(
                        f"Task {item_id}/{band_name} attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"All {max_retries} attempts failed for {item_id}/{band_name}. Last error: {e}"
                    )
                    return item_id, band_name, [None] * len(coords_4326)

    # execute all tasks in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(fetch_with_retry, tasks))

    # group results by item_id and reconstruct dataframes
    logger.info("grouping results by item")
    results_by_item = defaultdict(dict)

    for item_id, band_name, values in results:
        results_by_item[item_id][band_name] = values

    # build dataframes for each item
    dfs = []
    for item_id, band_dict in results_by_item.items():
        band_dict_with_coords = band_dict.copy()
        band_dict_with_coords.update(
            pts_clipped.reset_index().drop(columns=["geometry"]).to_dict(orient="list")
        )
        df = pd.DataFrame(band_dict_with_coords).assign(
            time=item_metadata[item_id]["datetime"],
            item_id=item_id,
        )
        dfs.append(df)

    # combine all batches
    logger.info("combining all batches into final dataframe")
    sample_df = pd.concat(dfs, axis=0)

    output_parquet = f"{output_dir}/point_sample.parquet"
    sample_df.to_parquet(output_parquet, compression="zstd")

    catalog = Catalog(
        id="DPS",
        description="DPS",
        catalog_type=CatalogType.SELF_CONTAINED,
    )

    item = Item(
        id="-".join(
            [
                mgrs_tile,
                start_datetime.strftime("%Y%m%d"),
                end_datetime.strftime("%Y%m%d"),
            ]
        ),
        bbox=list(stack_bbox),
        geometry=mapping(stack_extent),
        datetime=end_datetime,
        properties={
            "start_datetime": start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_datetime": end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        assets={
            "parquet": Asset(
                href=output_parquet.replace(f"{output_dir}/", ""),
                title="HLS time series point sample",
                description="HLS time series point sample",
                extra_fields={"created": datetime.now(tz=UTC).isoformat()},
            )
        },
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
    args = parse.parse_args()

    output_dir = Path(args.output_dir)
    start_datetime = parse_datetime_utc(args.start_datetime)
    end_datetime = parse_datetime_utc(args.end_datetime)

    logger.info(
        f"setting GDAL config environment variables:\n{json.dumps(GDAL_CONFIG, indent=2)}"
    )
    os.environ.update(GDAL_CONFIG)

    logger.info(
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
    )
