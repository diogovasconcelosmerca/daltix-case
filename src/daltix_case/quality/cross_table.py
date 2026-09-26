# Validate critical relationships between the final Silver datasets.
# These contracts ensure that individually valid tables also behave safely together.

from pathlib import Path

import duckdb

from daltix_case.quality.checks import (
    fail_if_false,
)
from daltix_case.silver import SILVER_FILES
from daltix_case.source_io import quote_literal


def run_cross_table_contracts(
    silver_dir: Path,
) -> dict[str, float | int]:
    """
    Validate critical relationships across the completed Silver layer.

    Hard failures:
        - unexpected row multiplication
        - incomplete weekly location references
        - product-context contradictions

    Coverage limitations are returned as metrics rather than hidden.
    """

    con = duckdb.connect()

    try:
        weekly_prices = quote_literal(
            (silver_dir / SILVER_FILES["weekly_prices"]).as_posix()
        )

        weekly_products = quote_literal(
            (silver_dir / SILVER_FILES["weekly_products"]).as_posix()
        )

        weekly_locations = quote_literal(
            (silver_dir / SILVER_FILES["weekly_locations"]).as_posix()
        )

        nutritionals = quote_literal(
            (silver_dir / SILVER_FILES["nutritionals"]).as_posix()
        )

        # Weekly Prices → Weekly Products:
        # incomplete coverage is allowed, but cardinality must remain safe.
        product_relationship = (
            con.sql(f"""
            WITH prices AS (
                SELECT *
                FROM read_parquet({weekly_prices})
            ),

            products AS (
                SELECT
                    daltix_id,
                    shop
                FROM read_parquet({weekly_products})
            ),

            joined AS (
                SELECT
                    p.*,
                    pr.daltix_id AS matched_product_id,
                    pr.shop AS product_shop

                FROM prices p

                LEFT JOIN products pr
                    ON p.daltix_id = pr.daltix_id
            )

            SELECT
                (SELECT COUNT(*) FROM prices)
                    AS price_rows,

                COUNT(*)
                    AS joined_rows,

                COUNT(*) FILTER (
                    WHERE matched_product_id IS NOT NULL
                ) AS matched_rows,

                COUNT(*) FILTER (
                    WHERE matched_product_id IS NULL
                ) AS unmatched_rows,

                COUNT(*) FILTER (
                    WHERE matched_product_id IS NOT NULL
                      AND shop IS DISTINCT FROM product_shop
                ) AS shop_context_mismatches,

                ROUND(
                    100.0
                    * COUNT(*) FILTER (
                        WHERE matched_product_id IS NOT NULL
                    )
                    / COUNT(*),
                    2
                ) AS coverage_pct

            FROM joined
        """)
            .pl()
            .row(
                0,
                named=True,
            )
        )

        # Weekly Prices → Weekly Locations:
        # this relationship must be complete and must not multiply rows.
        location_relationship = (
            con.sql(f"""
            WITH prices AS (
                SELECT *
                FROM read_parquet({weekly_prices})
            ),

            locations AS (
                SELECT
                    shop,
                    location
                FROM read_parquet({weekly_locations})
            ),

            joined AS (
                SELECT
                    p.*,
                    l.location AS matched_location

                FROM prices p

                LEFT JOIN locations l
                    ON p.shop = l.shop
                   AND p.location = l.location
            )

            SELECT
                (SELECT COUNT(*) FROM prices)
                    AS price_rows,

                COUNT(*)
                    AS joined_rows,

                COUNT(*) FILTER (
                    WHERE matched_location IS NOT NULL
                ) AS matched_rows,

                COUNT(*) FILTER (
                    WHERE matched_location IS NULL
                ) AS unmatched_rows

            FROM joined
        """)
            .pl()
            .row(
                0,
                named=True,
            )
        )

        # Nutritionals → Weekly Products:
        # overlap may remain limited, but matched product context must agree.
        nutrition_relationship = (
            con.sql(f"""
            WITH nutrition_products AS (
                SELECT DISTINCT
                    daltix_id,
                    shop,
                    country

                FROM read_parquet({nutritionals})
            ),

            weekly_products_ref AS (
                SELECT
                    daltix_id,
                    shop,
                    country

                FROM read_parquet({weekly_products})
            )

            SELECT
                COUNT(DISTINCT n.daltix_id) FILTER (
                    WHERE wp.daltix_id IS NOT NULL
                ) AS matched_products,

                COUNT(*) FILTER (
                    WHERE wp.daltix_id IS NOT NULL
                      AND (
                          n.shop IS DISTINCT FROM wp.shop
                          OR n.country IS DISTINCT FROM wp.country
                      )
                ) AS context_mismatches

            FROM nutrition_products n

            LEFT JOIN weekly_products_ref wp
                ON n.daltix_id = wp.daltix_id
        """)
            .pl()
            .row(
                0,
                named=True,
            )
        )

        # Define only genuinely structural relationship failures as hard contracts.
        contract = {
            "weekly_prices_not_empty": product_relationship["price_rows"] > 0,
            "weekly_product_join_no_row_explosion": product_relationship["joined_rows"]
            == product_relationship["price_rows"],
            "weekly_product_context_consistent": product_relationship[
                "shop_context_mismatches"
            ]
            == 0,
            "weekly_location_join_no_row_explosion": location_relationship[
                "joined_rows"
            ]
            == location_relationship["price_rows"],
            "weekly_location_reference_complete": location_relationship[
                "unmatched_rows"
            ]
            == 0,
            "nutrition_product_context_consistent": nutrition_relationship[
                "context_mismatches"
            ]
            == 0,
        }

        fail_if_false(
            contract,
            "Cross-Table Silver",
        )

        return {
            "weekly_price_rows": int(product_relationship["price_rows"]),
            "weekly_product_joined_rows": int(product_relationship["joined_rows"]),
            "weekly_product_matched_rows": int(product_relationship["matched_rows"]),
            "weekly_product_context_mismatches": int(
                product_relationship["shop_context_mismatches"]
            ),
            "weekly_location_joined_rows": int(location_relationship["joined_rows"]),
            "nutrition_product_context_mismatches": int(
                nutrition_relationship["context_mismatches"]
            ),
            "weekly_product_coverage_pct": float(product_relationship["coverage_pct"]),
            "weekly_product_unmatched_rows": int(
                product_relationship["unmatched_rows"]
            ),
            "weekly_location_unmatched_rows": int(
                location_relationship["unmatched_rows"]
            ),
            "matched_nutritional_products": int(
                nutrition_relationship["matched_products"]
            ),
        }

    finally:
        con.close()
