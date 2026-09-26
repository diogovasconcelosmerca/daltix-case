# Build the Silver Products dataset from the immutable Raw products source.
# This module contains the production version of the Products logic validated in notebook 02.

from pathlib import Path

import polars as pl

from daltix_case.quality.checks import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_unique,
)

PRODUCT_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]

PRODUCT_TEXT_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]


def run_products_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """
    Build the Silver Products dataset.

    Grain:
        One row per daltix_id.

    Business key:
        daltix_id.

    Returns:
        A small dictionary of pipeline metrics for orchestration/logging.
    """

    # Load the immutable Raw source and validate its expected schema.
    protect_inputs(raw_path, silver_path)
    products_raw = pl.read_parquet(raw_path)

    require_columns(
        products_raw,
        PRODUCT_REQUIRED_COLUMNS,
    )

    # Normalize text and convert semantic missing values to actual NULLs.
    # Legitimate missing descriptive attributes remain NULL.
    products_clean = products_raw.with_columns(
        [normalize_text(column) for column in PRODUCT_TEXT_COLUMNS]
    )

    # Validate the business key before materializing the dataset.
    # Products must remain safe to use later as an enrichment source.
    require_unique(
        products_clean,
        ["daltix_id"],
        "Products",
    )

    # Define structural invariants that must hold for the pipeline to succeed.
    products_contract = {
        "row_count_preserved": products_clean.height == products_raw.height,
        "daltix_id_complete": products_clean["daltix_id"].null_count() == 0,
        "shop_complete": products_clean["shop"].null_count() == 0,
        "country_complete": products_clean["country"].null_count() == 0,
        "language_complete": products_clean["language"].null_count() == 0,
        "daltix_id_unique": products_clean["daltix_id"].n_unique()
        == products_clean.height,
    }

    # Stop the pipeline before writing if a critical contract fails.
    fail_if_false(
        products_contract,
        "Products",
    )

    # Ensure the output directory exists.
    silver_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Materialize the validated Silver dataset.
    products_clean.write_parquet(
        silver_path,
        compression="zstd",
    )

    # Re-read the materialized output to verify row count and business-key uniqueness.
    products_written = (
        pl.scan_parquet(silver_path)
        .select(
            [
                pl.len().alias("rows"),
                pl.col("daltix_id").n_unique().alias("unique_products"),
            ]
        )
        .collect()
    )

    written_rows = products_written["rows"][0]
    written_products = products_written["unique_products"][0]

    if written_rows != products_clean.height:
        raise RuntimeError(
            "Products post-write validation failed: "
            "row count changed during materialization."
        )

    if written_products != written_rows:
        raise RuntimeError(
            "Products post-write validation failed: daltix_id is not unique."
        )

    # Return useful metrics to the master pipeline.
    return {
        "rows": written_rows,
        "missing_name": products_clean["name"].null_count(),
        "missing_brand": products_clean["brand"].null_count(),
        "missing_description": products_clean["description"].null_count(),
        "missing_categories": products_clean["categories"].null_count(),
    }
