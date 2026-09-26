"""Clean weekly locations; enrich only missing fields from compatible references."""

from pathlib import Path

import polars as pl

from daltix_case.quality.checks import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_finite,
    require_unique,
)

WEEKLY_LOCATION_REQUIRED_COLUMNS = [
    "shop",
    "location",
    "location_name",
    "shop_type",
    "geolocation_latitude",
    "geolocation_longitude",
    "locality",
    "postcode",
    "state",
]
WEEKLY_LOCATION_TEXT_COLUMNS = [
    "shop",
    "location",
    "location_name",
    "shop_type",
    "locality",
    "postcode",
    "state",
]
COORDINATE_TOLERANCE = 0.00001


def run_weekly_locations_pipeline(
    raw_path: Path,
    fallback_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """Preserve one row per shop/location and never overwrite primary attributes."""
    protect_inputs(raw_path, silver_path, fallback_path)
    raw = pl.read_parquet(raw_path)
    fallback = pl.read_parquet(fallback_path)
    require_columns(raw, WEEKLY_LOCATION_REQUIRED_COLUMNS)
    require_columns(
        fallback,
        [
            "shop",
            "id",
            "country_code",
            "postcode",
            "geolocation_latitude",
            "geolocation_longitude",
            "is_enrichment_safe",
        ],
        exact=False,
    )
    clean = raw.with_columns(
        [normalize_text(c) for c in WEEKLY_LOCATION_TEXT_COLUMNS]
    ).with_columns(
        [
            pl.col("geolocation_latitude").cast(pl.Float64),
            pl.col("geolocation_longitude").cast(pl.Float64),
        ]
    )
    require_unique(clean, ["shop", "location"], "Weekly Locations")
    require_finite(clean, ["geolocation_latitude", "geolocation_longitude"])

    safe = (
        fallback.filter(pl.col("is_enrichment_safe"))
        .select(
            [
                "shop",
                "id",
                "country_code",
                "postcode",
                "geolocation_latitude",
                "geolocation_longitude",
            ]
        )
        .rename(
            {
                "id": "location",
                "country_code": "fallback_country_code",
                "postcode": "fallback_postcode",
                "geolocation_latitude": "fallback_latitude",
                "geolocation_longitude": "fallback_longitude",
            }
        )
    )
    joined = clean.join(safe, on=["shop", "location"], how="left", validate="1:1")

    # Detect contradictions on original primary attributes before any filling.
    joined = (
        joined.with_columns(
            [
                (
                    pl.col("postcode").is_not_null()
                    & pl.col("fallback_postcode").is_not_null()
                    & (pl.col("postcode") != pl.col("fallback_postcode"))
                )
                .fill_null(False)
                .alias("dq_postcode_conflict"),
                (
                    pl.col("geolocation_latitude").is_not_null()
                    & pl.col("fallback_latitude").is_not_null()
                    & (
                        (
                            pl.col("geolocation_latitude") - pl.col("fallback_latitude")
                        ).abs()
                        > COORDINATE_TOLERANCE
                    )
                )
                .fill_null(False)
                .alias("dq_latitude_conflict"),
                (
                    pl.col("geolocation_longitude").is_not_null()
                    & pl.col("fallback_longitude").is_not_null()
                    & (
                        (
                            pl.col("geolocation_longitude")
                            - pl.col("fallback_longitude")
                        ).abs()
                        > COORDINATE_TOLERANCE
                    )
                )
                .fill_null(False)
                .alias("dq_longitude_conflict"),
            ]
        )
        .with_columns(
            pl.any_horizontal(
                "dq_postcode_conflict", "dq_latitude_conflict", "dq_longitude_conflict"
            ).alias("dq_enrichment_context_mismatch")
        )
        .with_columns(
            (
                pl.col("fallback_country_code").is_not_null()
                & ~pl.col("dq_enrichment_context_mismatch")
            ).alias("_context_safe")
        )
    )
    enriched = joined.with_columns(
        [
            (
                pl.col("_context_safe")
                & pl.col("postcode").is_null()
                & pl.col("fallback_postcode").is_not_null()
            ).alias("is_postcode_enriched"),
            (
                pl.col("_context_safe")
                & pl.col("geolocation_latitude").is_null()
                & pl.col("geolocation_longitude").is_null()
                & pl.col("fallback_latitude").is_not_null()
                & pl.col("fallback_longitude").is_not_null()
            ).alias("is_coordinates_enriched"),
            pl.when(pl.col("_context_safe"))
            .then(pl.col("fallback_country_code"))
            .otherwise(None)
            .alias("country_code"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("is_postcode_enriched"))
            .then(pl.col("fallback_postcode"))
            .otherwise(pl.col("postcode"))
            .alias("postcode"),
            pl.when(pl.col("is_coordinates_enriched"))
            .then(pl.col("fallback_latitude"))
            .otherwise(pl.col("geolocation_latitude"))
            .alias("geolocation_latitude"),
            pl.when(pl.col("is_coordinates_enriched"))
            .then(pl.col("fallback_longitude"))
            .otherwise(pl.col("geolocation_longitude"))
            .alias("geolocation_longitude"),
        ]
    )
    require_finite(enriched, ["geolocation_latitude", "geolocation_longitude"])
    # Flags describe final geography, including any accepted fallback values.
    enriched = enriched.with_columns(
        [
            (~pl.col("geolocation_latitude").is_between(-90, 90))
            .fill_null(False)
            .alias("dq_invalid_latitude"),
            (~pl.col("geolocation_longitude").is_between(-180, 180))
            .fill_null(False)
            .alias("dq_invalid_longitude"),
            (
                pl.col("geolocation_latitude").is_null()
                != pl.col("geolocation_longitude").is_null()
            ).alias("dq_incomplete_coordinates"),
        ]
    )
    result = enriched.drop(
        [
            "fallback_country_code",
            "fallback_postcode",
            "fallback_latitude",
            "fallback_longitude",
            "_context_safe",
        ]
    )
    fail_if_false(
        {
            "row_count_preserved": result.height == raw.height,
            "shop_complete": result["shop"].null_count() == 0,
            "location_complete": result["location"].null_count() == 0,
            "business_key_unique": result.select("shop", "location").unique().height
            == result.height,
            "valid_latitudes": result["dq_invalid_latitude"].sum() == 0,
            "valid_longitudes": result["dq_invalid_longitude"].sum() == 0,
            "coordinate_pairs_consistent": result["dq_incomplete_coordinates"].sum()
            == 0,
            "no_unsafe_enrichment": result.filter(
                pl.col("dq_enrichment_context_mismatch")
                & (
                    pl.col("is_postcode_enriched")
                    | pl.col("is_coordinates_enriched")
                    | pl.col("country_code").is_not_null()
                )
            ).height
            == 0,
        },
        "Weekly Locations",
    )
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_parquet(silver_path, compression="zstd")
    written = (
        pl.scan_parquet(silver_path)
        .select(
            pl.len().alias("rows"),
            pl.struct("shop", "location").n_unique().alias("keys"),
        )
        .collect()
        .row(0, named=True)
    )
    if written["rows"] != result.height or written["keys"] != result.height:
        raise RuntimeError(
            "Weekly Locations post-write row count or key validation failed."
        )
    return {
        "rows": result.height,
        "safe_fallback_matches": int(joined["_context_safe"].sum()),
        "context_mismatches": int(result["dq_enrichment_context_mismatch"].sum()),
        "postcodes_enriched": int(result["is_postcode_enriched"].sum()),
        "coordinates_enriched": int(result["is_coordinates_enriched"].sum()),
        "countries_enriched": result["country_code"].len()
        - result["country_code"].null_count(),
        "remaining_missing_postcode": result["postcode"].null_count(),
        "remaining_missing_coordinates": result["geolocation_latitude"].null_count(),
    }
