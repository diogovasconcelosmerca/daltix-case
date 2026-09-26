# Build the Silver Weekly Products dataset from the Raw weekly product source.
# The validated Silver Products dataset is retained as a production-ready fallback
# enrichment source when future source overlap can safely recover missing attributes.

from pathlib import Path

import polars as pl

from daltix_case.quality.checks import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_unique,
)

WEEKLY_PRODUCT_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]

WEEKLY_PRODUCT_TEXT_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]

ENRICHMENT_ATTRIBUTES = [
    "name",
    "brand",
    "description",
    "categories",
]


def run_weekly_products_pipeline(
    raw_path: Path,
    fallback_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """
    Build the Silver Weekly Products dataset.

    Grain:
        One row per daltix_id.

    Primary source:
        weekly_prices_products.

    Fallback:
        Silver products.

    Existing weekly values always remain authoritative.
    """

    # Load the authoritative weekly source and validated fallback source.
    protect_inputs(raw_path, silver_path, fallback_path)
    weekly_raw = pl.read_parquet(raw_path)
    fallback = pl.read_parquet(fallback_path)

    require_columns(
        weekly_raw,
        WEEKLY_PRODUCT_REQUIRED_COLUMNS,
    )

    # Normalize text and semantic NULL representations.
    weekly_clean = weekly_raw.with_columns(
        [normalize_text(column) for column in WEEKLY_PRODUCT_TEXT_COLUMNS]
    )

    # The weekly product identifier must remain unique before enrichment.
    require_unique(
        weekly_clean,
        ["daltix_id"],
        "Weekly Products",
    )

    # Prepare fallback attributes using explicit names to preserve provenance.
    fallback_prepared = fallback.select(
        [
            "daltix_id",
            "shop",
            "country",
            "language",
            "name",
            "brand",
            "description",
            "categories",
        ]
    ).rename(
        {
            "shop": "fallback_shop",
            "country": "fallback_country",
            "language": "fallback_language",
            "name": "fallback_name",
            "brand": "fallback_brand",
            "description": "fallback_description",
            "categories": "fallback_categories",
        }
    )

    # Join by daltix_id while enforcing one-to-one cardinality.
    joined = (
        weekly_clean.join(
            fallback_prepared,
            on="daltix_id",
            how="left",
            validate="1:1",
        )
        .with_columns(
            [
                pl.col("fallback_shop").is_not_null().alias("_has_fallback"),
                (
                    (pl.col("shop") == pl.col("fallback_shop"))
                    & (pl.col("country") == pl.col("fallback_country"))
                    & (pl.col("language") == pl.col("fallback_language"))
                )
                .fill_null(False)
                .alias("_context_safe"),
            ]
        )
        .with_columns(
            (pl.col("_has_fallback") & ~pl.col("_context_safe")).alias(
                "dq_enrichment_context_mismatch"
            )
        )
    )

    # Enrich only missing attributes from context-compatible fallback records.
    # Populated weekly values are never overwritten.
    enriched = joined

    for attribute in ENRICHMENT_ATTRIBUTES:
        fallback_column = f"fallback_{attribute}"

        enriched = enriched.with_columns(
            [
                (
                    pl.col(attribute).is_null()
                    & pl.col(fallback_column).is_not_null()
                    & pl.col("_context_safe")
                ).alias(f"is_{attribute}_enriched"),
                (
                    pl.col(attribute).is_not_null()
                    & pl.col(fallback_column).is_not_null()
                    & pl.col("_context_safe")
                    & (pl.col(attribute) != pl.col(fallback_column))
                ).alias(f"dq_{attribute}_conflict"),
            ]
        ).with_columns(
            pl.when(pl.col(f"is_{attribute}_enriched"))
            .then(pl.col(fallback_column))
            .otherwise(pl.col(attribute))
            .alias(attribute)
        )

    name_case_only = enriched.filter(
        pl.col("dq_name_conflict")
        & (
            pl.col("name").str.to_lowercase()
            == pl.col("fallback_name").str.to_lowercase()
        )
    ).height
    fallback_matches = int(joined["_has_fallback"].sum())
    safe_matches = int(joined["_context_safe"].sum())

    # Remove temporary fallback fields while retaining enrichment/DQ provenance.
    weekly_silver = enriched.drop(
        [
            "fallback_shop",
            "fallback_country",
            "fallback_language",
            "fallback_name",
            "fallback_brand",
            "fallback_description",
            "fallback_categories",
            "_has_fallback",
            "_context_safe",
        ]
    )

    # Enrichment may improve completeness but must never change the product grain.
    contract = {
        "row_count_preserved": weekly_silver.height == weekly_raw.height,
        "daltix_id_complete": weekly_silver["daltix_id"].null_count() == 0,
        "shop_complete": weekly_silver["shop"].null_count() == 0,
        "country_complete": weekly_silver["country"].null_count() == 0,
        "language_complete": weekly_silver["language"].null_count() == 0,
        "daltix_id_unique": weekly_silver["daltix_id"].n_unique()
        == weekly_silver.height,
        "no_unsafe_enrichment": (
            weekly_silver.filter(
                pl.col("dq_enrichment_context_mismatch")
                & (
                    pl.col("is_name_enriched")
                    | pl.col("is_brand_enriched")
                    | pl.col("is_description_enriched")
                    | pl.col("is_categories_enriched")
                )
            ).height
            == 0
        ),
    }

    fail_if_false(
        contract,
        "Weekly Products",
    )

    # Materialize the validated output.
    silver_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    weekly_silver.write_parquet(
        silver_path,
        compression="zstd",
    )

    # Validate the persisted business grain.
    written = (
        pl.scan_parquet(silver_path)
        .select(
            [
                pl.len().alias("rows"),
                pl.col("daltix_id").n_unique().alias("unique_products"),
            ]
        )
        .collect()
    )

    rows = int(written["rows"][0])
    unique_products = int(written["unique_products"][0])

    if rows != weekly_silver.height:
        raise RuntimeError("Weekly Products post-write row count changed.")

    if rows != unique_products:
        raise RuntimeError(
            "Weekly Products post-write validation failed: daltix_id is not unique."
        )

    return {
        "rows": rows,
        "fallback_matches": fallback_matches,
        "safe_fallback_matches": safe_matches,
        "name_literal_differences": int(weekly_silver["dq_name_conflict"].sum()),
        "name_case_only_differences": name_case_only,
        "brand_literal_differences": int(weekly_silver["dq_brand_conflict"].sum()),
        "description_literal_differences": int(
            weekly_silver["dq_description_conflict"].sum()
        ),
        "categories_literal_differences": int(
            weekly_silver["dq_categories_conflict"].sum()
        ),
        "context_mismatches": int(
            weekly_silver["dq_enrichment_context_mismatch"].sum()
        ),
        "remaining_missing_name": weekly_silver["name"].null_count(),
        "names_enriched": int(weekly_silver["is_name_enriched"].sum()),
        "brands_enriched": int(weekly_silver["is_brand_enriched"].sum()),
        "descriptions_enriched": int(weekly_silver["is_description_enriched"].sum()),
        "categories_enriched": int(weekly_silver["is_categories_enriched"].sum()),
        "remaining_missing_brand": weekly_silver["brand"].null_count(),
        "remaining_missing_description": weekly_silver["description"].null_count(),
        "remaining_missing_categories": weekly_silver["categories"].null_count(),
    }
