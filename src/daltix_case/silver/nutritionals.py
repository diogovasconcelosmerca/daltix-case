"""Canonical nutritional observations with explicit selection evidence and portion basis."""

from pathlib import Path

import duckdb
import polars as pl

from daltix_case.quality.checks import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_date,
)
from daltix_case.source_io import quote_literal

NUTRITIONALS_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "download_date",
    "nutritional_values_std",
]
NUTRITIONALS_TEXT_COLUMNS = ["daltix_id", "shop", "country", "language"]
NUTRITIONALS_EXACT_KEY = NUTRITIONALS_REQUIRED_COLUMNS
# Unit mismatches are review flags, never automatic conversions.
NUTRIENT_UNITS = {
    "carbohydrates": "g",
    "energy": "kJ",
    "fats": "g",
    "fibers": "g",
    "kilocalories": "kcal",
    "monounsaturated_fats": "g",
    "polyols": "g",
    "polyunsaturated_fats": "g",
    "proteins": "g",
    "salt": "g",
    "saturated_fats": "g",
    "starch": "g",
    "sugars": "g",
    "unsaturated_fats": "g",
}

CANONICAL_QUERY = """
        WITH nutrient_stats AS (
            SELECT
                n._source_row_id,

                COUNT(nutrient.key)
                    AS nutrient_count,

                COUNT(*) FILTER (
                    WHERE TRY_CAST(
                        json_extract_string(
                            nutrient.value,
                            '$.value'
                        )
                        AS DOUBLE
                    ) IS NOT NULL
                ) AS numeric_nutrient_count,

                COUNT(*) FILTER (
                    WHERE NULLIF(
                        TRIM(
                            json_extract_string(
                                nutrient.value,
                                '$.unit'
                            )
                        ),
                        ''
                    ) IS NOT NULL
                ) AS populated_unit_count

            FROM nutritionals_deduped_source n

            LEFT JOIN LATERAL json_each(
                n.nutritional_values_std,
                '$.nutrients'
            ) nutrient
                ON TRUE

            GROUP BY n._source_row_id
        ),

        grain_stats AS (
            SELECT
                daltix_id,
                shop,
                country,
                download_date,

                COUNT(*) AS grain_row_count,

                COUNT(DISTINCT nutritional_values_std)
                    AS nutritional_versions,

                COUNT(DISTINCT language)
                    AS language_versions

            FROM nutritionals_deduped_source

            GROUP BY
                daltix_id,
                shop,
                country,
                download_date
        ),

        ranked AS (
            SELECT
                n.*,

                s.nutrient_count,
                s.numeric_nutrient_count,
                s.populated_unit_count,

                g.grain_row_count,

                g.grain_row_count > 1
                    AS dq_repeated_grain,

                g.nutritional_versions > 1
                    AS dq_conflicting_nutrition,

                g.language_versions > 1
                    AS dq_language_conflict,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        n.daltix_id,
                        n.shop,
                        n.country,
                        n.download_date

                    ORDER BY
                        s.numeric_nutrient_count DESC,
                        s.nutrient_count DESC,
                        s.populated_unit_count DESC,
                        md5(n.nutritional_values_std) ASC,
                        COALESCE(n.language, '') ASC,
                        n._source_row_id ASC
                ) AS canonical_rank,
                COUNT(*) OVER (
                    PARTITION BY n.daltix_id, n.shop, n.country, n.download_date,
                        s.numeric_nutrient_count, s.nutrient_count, s.populated_unit_count
                ) AS completeness_tie_count

            FROM nutritionals_deduped_source n

            INNER JOIN nutrient_stats s
                USING (_source_row_id)

            INNER JOIN grain_stats g
                USING (
                    daltix_id,
                    shop,
                    country,
                    download_date
                )
        )

        SELECT
            *,

            CASE
                WHEN grain_row_count > 1
                    THEN 'completeness_first'

                WHEN dq_source_exact_duplicate
                    THEN 'exact_duplicate_collapsed'

                ELSE 'single_observation'
            END AS nutrition_resolution_rule

        FROM ranked

        WHERE canonical_rank = 1
        """


def run_nutritionals_pipeline(raw_path: Path, silver_path: Path) -> dict[str, int]:
    """Keep the completeness/hash policy; preserve uncertainty and nutrient context.

    Source grain: product/shop/country/date. Silver adds nutrient_name.
    A selected version is reproducible, not proven true. Raw retains all versions.
    """
    protect_inputs(raw_path, silver_path)
    raw = pl.read_parquet(raw_path)
    require_columns(raw, NUTRITIONALS_REQUIRED_COLUMNS)
    require_date(raw, "download_date")
    clean = raw.with_columns([normalize_text(c) for c in NUTRITIONALS_TEXT_COLUMNS])
    fail_if_false(
        {
            f"{c}_complete": clean[c].null_count() == 0
            for c in NUTRITIONALS_TEXT_COLUMNS
            + ["download_date", "nutritional_values_std"]
        },
        "Nutritionals source",
    )

    with duckdb.connect() as con:
        con.register("source", clean)
        valid = con.sql(
            "SELECT count(*) FILTER (WHERE NOT json_valid(nutritional_values_std)) FROM source"
        ).fetchone()[0]
        fail_if_false({"json_valid": valid == 0}, "Nutritionals JSON")
        structure = con.sql("""
            SELECT count(*) FILTER (WHERE
                json_type(nutritional_values_std, '$.nutrients') IS DISTINCT FROM 'OBJECT'
                OR coalesce(len(json_keys(nutritional_values_std, '$.nutrients')), 0) = 0
                OR try_cast(json_extract_string(nutritional_values_std, '$.portion.value') AS DOUBLE) IS NULL
                OR NOT isfinite(try_cast(json_extract_string(nutritional_values_std, '$.portion.value') AS DOUBLE))
                OR try_cast(json_extract_string(nutritional_values_std, '$.portion.value') AS DOUBLE) <= 0
                OR nullif(trim(json_extract_string(nutritional_values_std, '$.portion.unit')), '') IS NULL
            ) FROM source
        """).fetchone()[0]
        fail_if_false(
            {"nonempty_nutrients_and_portion": structure == 0}, "Nutritionals structure"
        )

        deduped = (
            clean.with_row_index("_source_row_id")
            .with_columns(
                pl.len().over(NUTRITIONALS_EXACT_KEY).alias("_exact_duplicate_count")
            )
            .unique(subset=NUTRITIONALS_EXACT_KEY, keep="first", maintain_order=True)
            .with_columns(
                (pl.col("_exact_duplicate_count") > 1).alias(
                    "dq_source_exact_duplicate"
                )
            )
        )
        exact_removed = clean.height - deduped.height
        con.register("nutritionals_deduped_source", deduped)
        # Compute expensive JSON completeness/ranking once for metrics and export.
        con.execute("CREATE TEMP TABLE canonical AS " + CANONICAL_QUERY)
        resolution = (
            con.sql("""
            SELECT count(*) AS canonical_observations,
                count(*) FILTER (WHERE dq_repeated_grain) AS repeated_grains,
                count(*) FILTER (WHERE dq_conflicting_nutrition) AS conflicts,
                count(*) FILTER (WHERE dq_language_conflict) AS language_conflicts,
                count(*) FILTER (WHERE dq_repeated_grain AND completeness_tie_count > 1) AS hash_tiebreaks,
                sum(nutrient_count) AS expected_nutrient_rows,
                sum(grain_row_count) AS represented_source_rows
            FROM canonical
        """)
            .pl()
            .row(0, named=True)
        )
        fail_if_false(
            {
                "canonical_observations_not_empty": resolution["canonical_observations"]
                > 0,
                "all_deduped_observations_accounted_for": resolution[
                    "represented_source_rows"
                ]
                == deduped.height,
            },
            "Nutritionals reconciliation",
        )
        units = (
            "CASE nutrient_name "
            + " ".join(
                f"WHEN {quote_literal(k)} THEN {quote_literal(v)}"
                for k, v in NUTRIENT_UNITS.items()
            )
            + " ELSE NULL END"
        )
        query = f"""
            WITH normalized AS (
                SELECT n.daltix_id, n.shop, n.country, n.language, n.download_date,
                    nutrient.key AS nutrient_name,
                    nullif(trim(json_extract_string(nutrient.value, '$.value')), '') AS nutrient_value_raw,
                    try_cast(json_extract_string(nutrient.value, '$.value') AS DOUBLE) AS nutrient_value,
                    nullif(trim(json_extract_string(nutrient.value, '$.unit')), '') AS nutrient_unit,
                    cast(json_extract_string(n.nutritional_values_std, '$.portion.value') AS DOUBLE) AS portion_value,
                    json_extract_string(n.nutritional_values_std, '$.portion.unit') AS portion_unit,
                    md5(n.nutritional_values_std) AS source_payload_hash,
                    n.nutrient_count, n.numeric_nutrient_count, n.populated_unit_count,
                    n.dq_source_exact_duplicate, n.dq_repeated_grain,
                    n.dq_conflicting_nutrition, n.dq_language_conflict,
                    n.nutrition_resolution_rule,
                    CASE WHEN n.dq_repeated_grain AND n.completeness_tie_count > 1
                        THEN 'deterministic_hash_tiebreak'
                        ELSE n.nutrition_resolution_rule END AS nutrition_selection_reason
                FROM canonical n
                CROSS JOIN LATERAL json_each(n.nutritional_values_std, '$.nutrients') nutrient
            )
            SELECT *, nutrient_value_raw IS NULL AS dq_missing_nutrient_value,
                nutrient_value_raw IS NOT NULL AND nutrient_value IS NULL AS dq_non_numeric_value,
                nutrient_unit IS DISTINCT FROM ({units}) OR ({units}) IS NULL AS dq_unexpected_nutrient_unit
            FROM normalized
        """
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        # The orchestrator supplies a staging path; the 9.5M rows stay out of Python.
        con.execute(
            f"COPY ({query}) TO {quote_literal(silver_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        con.read_parquet(str(silver_path)).create_view("written")
        metrics = (
            con.sql("""
            SELECT count(*) AS rows, count(DISTINCT daltix_id) AS distinct_products,
                count(DISTINCT nutrient_name) AS distinct_nutrients,
                count(*) FILTER (WHERE nutrient_value IS NULL) AS missing_numeric_values,
                count(*) FILTER (WHERE dq_non_numeric_value) AS non_numeric_values,
                count(*) FILTER (WHERE nutrient_unit IS NULL) AS missing_units,
                count(*) FILTER (WHERE dq_unexpected_nutrient_unit) AS unexpected_unit_rows,
                count(*) FILTER (WHERE dq_conflicting_nutrition) AS rows_from_resolved_conflicts,
                count(*) FILTER (WHERE nutrient_value IS NOT NULL AND NOT isfinite(nutrient_value)) AS non_finite_values,
                count(*) FILTER (WHERE nutrient_name IS NULL OR trim(nutrient_name) = '') AS missing_nutrient_names,
                count(*) - count(DISTINCT (daltix_id, shop, country, download_date, nutrient_name)) AS duplicate_grains,
                count(DISTINCT (daltix_id, shop, country, download_date)) AS written_observations
            FROM written
        """)
            .pl()
            .row(0, named=True)
        )
        fail_if_false(
            {
                "row_count_reconciled": metrics["rows"]
                == resolution["expected_nutrient_rows"],
                "observation_count_reconciled": metrics["written_observations"]
                == resolution["canonical_observations"],
                "normalized_grain_unique": metrics["duplicate_grains"] == 0,
                "numeric_values_finite": metrics["non_finite_values"] == 0,
                "nutrient_names_complete": metrics["missing_nutrient_names"] == 0,
            },
            "Nutritionals output",
        )
        return {
            **{
                k: int(metrics[k])
                for k in [
                    "rows",
                    "distinct_products",
                    "distinct_nutrients",
                    "missing_numeric_values",
                    "non_numeric_values",
                    "missing_units",
                    "unexpected_unit_rows",
                    "rows_from_resolved_conflicts",
                ]
            },
            "exact_duplicates_removed": exact_removed,
            "canonical_observations": int(resolution["canonical_observations"]),
            # Legacy metric names are retained; they count canonicalized conflicts, not proven resolutions.
            "resolved_repeated_grains": int(resolution["repeated_grains"]),
            "resolved_nutritional_conflicts": int(resolution["conflicts"]),
            "resolved_language_conflicts": int(resolution["language_conflicts"]),
            "hash_tiebreak_grains": int(resolution["hash_tiebreaks"]),
        }
