# Build the core Silver Weekly Prices dataset directly from Parquet using DuckDB.
# True exact duplicates are collapsed, while multiple different price observations
# within the same weekly business grain are preserved and explicitly flagged.

from pathlib import Path

import duckdb

from daltix_case.quality.checks import (
    SEMANTIC_NULLS,
    fail_if_false,
    protect_inputs,
)
from daltix_case.source_io import quote_literal


def run_weekly_prices_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """
    Build the Silver Weekly Prices dataset.

    Expected business grain:
        daltix_id + shop + location + week.

    Exact duplicate rows:
        Collapsed.

    Different prices within the same weekly grain:
        Preserved and marked as unresolved price ambiguity.
    """

    protect_inputs(raw_path, silver_path)
    con = duckdb.connect()

    try:
        raw_path_sql = quote_literal(raw_path.as_posix())
        silver_path_sql = quote_literal(silver_path.as_posix())

        # Validate the required source schema without loading 19M rows into Python.
        required_columns = {
            "daltix_id",
            "shop",
            "location",
            "week",
            "price",
            "price_promo",
        }

        schema = {
            row[0]: row[1]
            for row in con.sql(
                f"DESCRIBE SELECT * FROM read_parquet({raw_path_sql})"
            ).fetchall()
        }
        if set(schema) != required_columns:
            raise ValueError(
                "Weekly Prices: source columns changed; review before deduplicating."
            )
        if schema["week"] != "DATE":
            raise ValueError(
                "Weekly Prices: week must be Date; timestamps require review."
            )

        # Standardize source fields directly from Parquet.
        null_tokens = ", ".join(quote_literal(token) for token in SEMANTIC_NULLS)
        clean_query = f"""
        SELECT
            CASE WHEN lower(trim(CAST(daltix_id AS VARCHAR))) IN ({null_tokens}) THEN NULL ELSE trim(CAST(daltix_id AS VARCHAR)) END AS daltix_id,
            CASE WHEN lower(trim(CAST(shop AS VARCHAR))) IN ({null_tokens}) THEN NULL ELSE trim(CAST(shop AS VARCHAR)) END AS shop,
            CASE WHEN lower(trim(CAST(location AS VARCHAR))) IN ({null_tokens}) THEN NULL ELSE trim(CAST(location AS VARCHAR)) END AS location,
            CAST(week AS DATE) AS week,
            CAST(price AS DOUBLE) AS price,
            CAST(price_promo AS DOUBLE) AS price_promo

        FROM read_parquet({raw_path_sql})
        """

        # Collapse only completely identical observations.
        deduped_query = f"""
        WITH cleaned AS ({clean_query})
        SELECT daltix_id, shop, location, week, price, price_promo,
            count(*) > 1 AS dq_source_exact_duplicate
        FROM cleaned
        GROUP BY daltix_id, shop, location, week, price, price_promo
        """

        # Preserve different observations within the same weekly grain
        # and make the resulting ambiguity explicit.
        silver_query = f"""
        WITH deduped AS (
            {deduped_query}
        ),

        profiled AS (
            SELECT
                *,

                COUNT(*) OVER (
                    PARTITION BY
                        daltix_id,
                        shop,
                        location,
                        week
                ) AS grain_observation_count,

                COUNT(*) OVER (
                    PARTITION BY daltix_id, shop, location, week
                ) AS price_version_count

            FROM deduped
        )

        SELECT
            daltix_id,
            shop,
            location,
            week,
            price,
            price_promo,

            md5(
                concat_ws(
                    '|',
                    daltix_id,
                    shop,
                    location,
                    CAST(week AS VARCHAR),
                    CAST(price AS VARCHAR),
                    CAST(price_promo AS VARCHAR)
                )
            ) AS price_observation_id,

            price_promo < price
                AS is_promotion,

            price <= 0
                AS dq_non_positive_price,

            price_promo <= 0
                AS dq_non_positive_promo_price,

            price_promo > price
                AS dq_promo_above_price,

            dq_source_exact_duplicate,

            grain_observation_count,
            price_version_count,

            grain_observation_count > 1
                AS dq_repeated_grain,

            price_version_count > 1
                AS dq_conflicting_price,

            NOT (price_version_count > 1)
                AS is_price_resolved,

            CASE
                WHEN price_version_count > 1
                    THEN 'multiple_price_observations'

                WHEN dq_source_exact_duplicate
                    THEN 'exact_duplicate_collapsed'

                ELSE 'single_observation'
            END AS price_resolution_rule

        FROM profiled
        """

        # The orchestrator publishes only after every staged output passes its contracts.
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        con.sql(
            f"COPY ({silver_query}) TO {silver_path_sql} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        # Validate the persisted transformation without recomputing its windows.
        contract_result = con.sql(f"""
        WITH silver AS (
            SELECT * FROM read_parquet({silver_path_sql})
        )

        SELECT
            COUNT(*) > 0
                AS dataset_not_empty,

            COUNT(*) FILTER (
                WHERE daltix_id IS NULL
                   OR shop IS NULL
                   OR location IS NULL
                   OR week IS NULL
                   OR price IS NULL
                   OR price_promo IS NULL
            ) = 0
                AS required_fields_complete,

            COUNT(*) FILTER (
                WHERE NOT isfinite(price) OR NOT isfinite(price_promo)
            ) = 0 AS prices_finite,

            COUNT(*) = COUNT(DISTINCT price_observation_id)
                AS observation_fingerprint_unique,

            COUNT(*) FILTER (
                WHERE dq_non_positive_price
            ) = 0
                AS regular_prices_positive,

            COUNT(*) FILTER (
                WHERE dq_non_positive_promo_price
            ) = 0
                AS promo_prices_positive,

            COUNT(*) - COUNT(
                DISTINCT struct_pack(
                    daltix_id := daltix_id,
                    shop := shop,
                    location := location,
                    week := week,
                    price := price,
                    price_promo := price_promo
                )
            ) = 0
                AS exact_duplicates_removed,

            COUNT(*) FILTER (
                WHERE dq_repeated_grain
                  AND NOT dq_conflicting_price
            ) = 0
                AS repeated_grains_are_explicitly_ambiguous,

            COUNT(*) FILTER (
                WHERE dq_conflicting_price
                  AND is_price_resolved
            ) = 0
                AS ambiguous_prices_never_marked_resolved

        FROM silver
        """).pl()

        contract = {
            column: bool(contract_result[column][0])
            for column in contract_result.columns
        }

        fail_if_false(
            contract,
            "Weekly Prices",
        )

        # Collect final metrics directly from the persisted Silver file.
        metrics = (
            con.sql(f"""
            WITH silver AS (
                SELECT *
                FROM read_parquet({silver_path_sql})
            ),

            raw AS (
                SELECT *
                FROM read_parquet({raw_path_sql})
            )

            SELECT
                (SELECT COUNT(*) FROM raw)
                    AS raw_rows,

                COUNT(*)
                    AS silver_rows,

                (SELECT COUNT(*) FROM raw)
                    - COUNT(*)
                    AS exact_duplicates_removed,

                COUNT(*) FILTER (
                    WHERE dq_conflicting_price
                ) AS ambiguous_rows,

                COUNT(
                    DISTINCT CASE
                        WHEN dq_conflicting_price
                        THEN struct_pack(
                            daltix_id := daltix_id,
                            shop := shop,
                            location := location,
                            week := week
                        )
                    END
                ) AS ambiguous_grains,

                COUNT(*) FILTER (
                    WHERE is_promotion
                ) AS promotion_rows,

                COUNT(*) FILTER (
                    WHERE dq_promo_above_price
                ) AS promo_above_price_rows

            FROM silver
        """)
            .pl()
            .row(
                0,
                named=True,
            )
        )

        return {
            "raw_rows": int(metrics["raw_rows"]),
            "rows": int(metrics["silver_rows"]),
            "exact_duplicates_removed": int(metrics["exact_duplicates_removed"]),
            "ambiguous_rows": int(metrics["ambiguous_rows"]),
            "ambiguous_grains": int(metrics["ambiguous_grains"]),
            "promotion_rows": int(metrics["promotion_rows"]),
            "promo_above_price_rows": int(metrics["promo_above_price_rows"]),
        }

    finally:
        con.close()
