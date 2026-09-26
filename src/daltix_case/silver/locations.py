# Build the Silver Locations dataset from the immutable Raw locations source.
# This module normalizes missing values, validates geography, handles ambiguous
# business keys safely, and prepares Locations for optional downstream enrichment.

from pathlib import Path

import polars as pl

from daltix_case.quality.checks import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_finite,
)

LOCATION_REQUIRED_COLUMNS = [
    "shop",
    "country_code",
    "id",
    "type",
    "geolocation_latitude",
    "geolocation_longitude",
    "postcode",
    "sources",
]

LOCATION_TEXT_COLUMNS = [
    "shop",
    "country_code",
    "id",
    "type",
    "postcode",
    "sources",
]


def run_locations_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """
    Build the Silver Locations dataset.

    Working grain:
        One location record within retailer context.

    Working business key:
        shop + id.

    Known business-key collisions are preserved and flagged rather than
    arbitrarily deduplicated.
    """

    # Load the immutable Raw source and validate the expected schema.
    protect_inputs(raw_path, silver_path)
    locations_raw = pl.read_parquet(raw_path)

    require_columns(
        locations_raw,
        LOCATION_REQUIRED_COLUMNS,
    )

    # Normalize text and semantic missing values while preserving source meaning.
    # Geographic coordinates are explicitly enforced as Float64.
    locations_stage = locations_raw.with_columns(
        [normalize_text(column) for column in LOCATION_TEXT_COLUMNS]
    ).with_columns(
        [
            pl.col("geolocation_latitude").cast(pl.Float64, strict=True),
            pl.col("geolocation_longitude").cast(pl.Float64, strict=True),
        ]
    )

    require_finite(locations_stage, ["geolocation_latitude", "geolocation_longitude"])

    # The provided source has no information in `type`.
    # Do not silently drop it if future data starts populating the field.
    type_has_information = locations_stage["type"].drop_nulls().len() > 0

    if type_has_information:
        raise RuntimeError(
            "Locations: `type` now contains information "
            "and must be reviewed before being removed."
        )

    locations_clean = locations_stage.drop("type")

    # Identify ambiguous shop + id keys.
    # Ambiguous records remain available but are excluded from automatic enrichment.
    locations_clean = (
        locations_clean.with_columns(
            pl.len().over(["shop", "id"]).alias("_business_key_count")
        )
        .with_columns(
            [
                (pl.col("_business_key_count") > 1).alias("dq_business_key_collision"),
                (pl.col("_business_key_count") == 1).alias("is_enrichment_safe"),
            ]
        )
        .drop("_business_key_count")
    )

    # Validate geographic structure without treating legitimate geographic
    # extremes or complete NULL coordinate pairs as errors.
    locations_clean = locations_clean.with_columns(
        [
            (
                pl.col("geolocation_latitude").is_not_null()
                & (
                    (pl.col("geolocation_latitude") < -90)
                    | (pl.col("geolocation_latitude") > 90)
                )
            ).alias("dq_invalid_latitude"),
            (
                pl.col("geolocation_longitude").is_not_null()
                & (
                    (pl.col("geolocation_longitude") < -180)
                    | (pl.col("geolocation_longitude") > 180)
                )
            ).alias("dq_invalid_longitude"),
            (
                pl.col("geolocation_latitude").is_null()
                != pl.col("geolocation_longitude").is_null()
            ).alias("dq_incomplete_coordinates"),
        ]
    )

    # Count the actual number of rows involved in duplicated business keys.
    collision_groups = (
        locations_clean.group_by(["shop", "id"]).len().filter(pl.col("len") > 1)
    )

    actual_collision_rows = (
        int(collision_groups["len"].sum()) if collision_groups.height > 0 else 0
    )

    flagged_collision_rows = int(locations_clean["dq_business_key_collision"].sum())

    # Define the critical structural contracts.
    # Missing postcode or complete missing coordinate pairs are allowed.
    locations_contract = {
        "row_count_preserved": locations_clean.height == locations_raw.height,
        "id_complete": locations_clean["id"].null_count() == 0,
        "shop_complete": locations_clean["shop"].null_count() == 0,
        "country_complete": locations_clean["country_code"].null_count() == 0,
        "empty_type_removed_safely": not type_has_information,
        "business_key_collisions_fully_flagged": flagged_collision_rows
        == actual_collision_rows,
        "enrichment_safety_consistent": (
            locations_clean.filter(
                pl.col("dq_business_key_collision") & pl.col("is_enrichment_safe")
            ).height
            == 0
        ),
        "valid_latitudes": locations_clean["dq_invalid_latitude"].sum() == 0,
        "valid_longitudes": locations_clean["dq_invalid_longitude"].sum() == 0,
        "coordinate_pairs_consistent": locations_clean[
            "dq_incomplete_coordinates"
        ].sum()
        == 0,
    }

    # Stop before writing if a critical structural rule fails.
    fail_if_false(
        locations_contract,
        "Locations",
    )

    # Ensure the target directory exists.
    silver_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Materialize the validated Silver dataset.
    locations_clean.write_parquet(
        silver_path,
        compression="zstd",
    )

    # Validate the materialized file after writing.
    locations_written = (
        pl.scan_parquet(silver_path).select(pl.len().alias("rows")).collect()
    )

    written_rows = int(locations_written["rows"][0])

    if written_rows != locations_clean.height:
        raise RuntimeError(
            "Locations post-write validation failed: "
            "row count changed during materialization."
        )

    # Return useful metrics to the master pipeline.
    return {
        "rows": written_rows,
        "business_key_collision_rows": flagged_collision_rows,
        "missing_postcode": locations_clean["postcode"].null_count(),
        "missing_coordinates": locations_clean["geolocation_latitude"].null_count(),
        "invalid_latitudes": int(locations_clean["dq_invalid_latitude"].sum()),
        "invalid_longitudes": int(locations_clean["dq_invalid_longitude"].sum()),
    }
