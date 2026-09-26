# Build the Silver Prices dataset from the immutable Raw prices source.
# This module normalizes the source, preserves meaningful promo NULLs,
# validates the historical business grain, derives promotion semantics,
# enforces structural contracts, and writes the validated Silver output.

from pathlib import Path

import polars as pl

from daltix_case.quality.checks import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_date,
    require_finite,
    require_unique,
)

PRICES_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "location",
    "price",
    "promo_price",
    "unit_std",
    "currency",
    "downloaded_on",
]

PRICES_TEXT_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "location",
    "unit_std",
    "currency",
]

PRICES_BUSINESS_KEY = [
    "daltix_id",
    "shop",
    "location",
    "downloaded_on",
]


def run_prices_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int | float]:
    """
    Build the Silver Prices dataset.

    Grain:
        One historical price observation per
        daltix_id + shop + location + downloaded_on.

    Business key:
        daltix_id + shop + location + downloaded_on.

    promo_price NULL is preserved because it represents meaningful
    source semantics rather than a value that should be imputed.
    """

    # Load the immutable Raw source and validate its expected schema.
    protect_inputs(raw_path, silver_path)
    prices_raw = pl.read_parquet(raw_path)

    require_columns(
        prices_raw,
        PRICES_REQUIRED_COLUMNS,
    )

    require_date(prices_raw, "downloaded_on")

    # Normalize identifiers and categorical fields while preserving
    # legitimate NULL promo_price values.
    prices_clean = prices_raw.with_columns(
        [normalize_text(column) for column in PRICES_TEXT_COLUMNS]
    ).with_columns(
        [
            pl.col("downloaded_on").cast(pl.Date, strict=True),
            pl.col("price").cast(pl.Float64, strict=True),
            pl.col("promo_price").cast(pl.Float64, strict=True),
        ]
    )

    require_finite(prices_clean, ["price", "promo_price"])

    # Make promotion semantics explicit and flag unexpected price behaviour.
    # Problematic rows are preserved rather than silently corrected.
    prices_clean = prices_clean.with_columns(
        [
            (
                pl.col("promo_price").is_not_null()
                & (pl.col("promo_price") < pl.col("price"))
            )
            .fill_null(False)
            .alias("is_promotion"),
            (pl.col("price") <= 0).fill_null(False).alias("dq_non_positive_price"),
            (pl.col("promo_price").is_not_null() & (pl.col("promo_price") <= 0))
            .fill_null(False)
            .alias("dq_non_positive_promo_price"),
            (
                pl.col("promo_price").is_not_null()
                & (pl.col("promo_price") >= pl.col("price"))
            )
            .fill_null(False)
            .alias("dq_promo_not_below_price"),
        ]
    )

    # Validate the historical business grain.
    # Duplicate observations at this key would make the source ambiguous.
    require_unique(
        prices_clean,
        PRICES_BUSINESS_KEY,
        "Prices",
    )

    # Define structural invariants that must remain true.
    # promo_price itself is intentionally allowed to remain NULL.
    prices_contract = {
        "row_count_preserved": prices_clean.height == prices_raw.height,
        "daltix_id_complete": prices_clean["daltix_id"].null_count() == 0,
        "shop_complete": prices_clean["shop"].null_count() == 0,
        "country_complete": prices_clean["country"].null_count() == 0,
        "location_complete": prices_clean["location"].null_count() == 0,
        "downloaded_on_complete": prices_clean["downloaded_on"].null_count() == 0,
        "price_complete": prices_clean["price"].null_count() == 0,
        "unit_std_complete": prices_clean["unit_std"].null_count() == 0,
        "currency_complete": prices_clean["currency"].null_count() == 0,
        "business_grain_unique": (
            prices_clean.select(PRICES_BUSINESS_KEY).unique().height
            == prices_clean.height
        ),
        "regular_prices_positive": prices_clean["dq_non_positive_price"].sum() == 0,
        "populated_promo_prices_positive": prices_clean[
            "dq_non_positive_promo_price"
        ].sum()
        == 0,
    }

    # Stop the pipeline before materialization if a critical contract fails.
    fail_if_false(
        prices_contract,
        "Prices",
    )

    # Ensure the output directory exists.
    silver_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Write the validated Silver dataset.
    prices_clean.write_parquet(
        silver_path,
        compression="zstd",
    )

    # Validate the materialized output after writing.
    prices_written = (
        pl.scan_parquet(silver_path).select(pl.len().alias("rows")).collect()
    )

    written_rows = int(prices_written["rows"][0])

    if written_rows != prices_clean.height:
        raise RuntimeError(
            "Prices post-write validation failed: "
            "row count changed during materialization."
        )

    # Return useful DQ and business metrics to the master pipeline.
    return {
        "rows": written_rows,
        "rows_without_promo_price": int(prices_clean["promo_price"].is_null().sum()),
        "promotion_rows": int(prices_clean["is_promotion"].sum()),
        "unexpected_promo_rows": int(prices_clean["dq_promo_not_below_price"].sum()),
        "distinct_products": prices_clean["daltix_id"].n_unique(),
        "distinct_locations": (
            prices_clean.select(["shop", "location"]).unique().height
        ),
    }
