"""Small business examples exercise the official builders, never notebook copies."""

import json
from datetime import date

import polars as pl
import pytest

from daltix_case.silver.nutritionals import run_nutritionals_pipeline
from daltix_case.silver.prices import run_prices_pipeline
from daltix_case.silver.weekly_locations import run_weekly_locations_pipeline
from daltix_case.silver.weekly_prices import run_weekly_prices_pipeline
from daltix_case.silver.weekly_products import run_weekly_products_pipeline


def write_raw(tmp_path, frame, name="source"):
    path = tmp_path / "raw" / f"{name}.parquet"
    path.parent.mkdir(exist_ok=True)
    frame.write_parquet(path)
    return path


def test_weekly_prices_preserves_alternatives_and_collapses_only_exact_rows(tmp_path):
    # An apostrophe in the path also exercises safe SQL literal handling.
    root = tmp_path / "reviewer's case"
    root.mkdir()
    data = pl.DataFrame(
        {
            "daltix_id": ["p"] * 3,
            "shop": ["s"] * 3,
            "location": ["l"] * 3,
            "week": [date(2020, 1, 6)] * 3,
            "price": [1.0, 1.0, 2.0],
            "price_promo": [1.0, 1.0, 1.5],
        }
    )
    raw = write_raw(root, data)
    out = root / "clean/silver_weekly_prices.parquet"
    metrics = run_weekly_prices_pipeline(raw, out)
    assert (
        metrics["rows"],
        metrics["exact_duplicates_removed"],
        metrics["ambiguous_grains"],
    ) == (2, 1, 1)
    result = pl.read_parquet(out)
    assert (
        result["dq_conflicting_price"].all() and not result["is_price_resolved"].any()
    )
    assert result["price_observation_id"].n_unique() == 2
    for invalid in [
        pl.lit(" ").alias("daltix_id"),
        pl.lit(float("nan")).alias("price"),
    ]:
        bad = write_raw(root, data.with_columns(invalid), "bad")
        with pytest.raises(RuntimeError, match="contract failed"):
            run_weekly_prices_pipeline(bad, root / "clean/bad.parquet")


def test_invalid_promo_does_not_become_no_promotion(tmp_path):
    frame = pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["s"],
            "country": ["be"],
            "location": ["l"],
            "downloaded_on": [date(2020, 1, 1)],
            "price": [1.0],
            "promo_price": ["invalid"],
            "unit_std": ["su"],
            "currency": ["eur"],
        }
    )
    with pytest.raises(pl.exceptions.InvalidOperationError):
        run_prices_pipeline(
            write_raw(tmp_path, frame), tmp_path / "clean/silver_prices.parquet"
        )
    raw = write_raw(
        tmp_path,
        frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("promo_price")),
    )
    out = tmp_path / "clean/silver_prices.parquet"
    run_prices_pipeline(raw, out)
    result = pl.read_parquet(out)
    assert result["promo_price"][0] is None and result["is_promotion"][0] is False


def test_product_enrichment_primary_wins_and_context_blocks_fallback(tmp_path):
    primary = pl.DataFrame(
        {
            "daltix_id": ["fill", "keep", "mismatch"],
            "shop": ["s"] * 3,
            "country": ["be"] * 3,
            "language": ["nl"] * 3,
            "name": ["A", "B", "C"],
            "brand": [None, "primary", None],
            "description": [None] * 3,
            "categories": [None] * 3,
        }
    )
    fallback = primary.with_columns(
        pl.lit("fallback").alias("brand"),
        pl.when(pl.col("daltix_id") == "mismatch")
        .then(pl.lit("fr"))
        .otherwise(pl.col("country"))
        .alias("country"),
    )
    raw = write_raw(tmp_path, primary)
    fall = write_raw(tmp_path, fallback, "fallback")
    out = tmp_path / "clean/silver_weekly_products.parquet"
    run_weekly_products_pipeline(raw, fall, out)
    result = pl.read_parquet(out).sort("daltix_id")
    assert dict(result.select("daltix_id", "brand").iter_rows()) == {
        "fill": "fallback",
        "keep": "primary",
        "mismatch": None,
    }
    assert result.filter(pl.col("daltix_id") == "mismatch")[
        "dq_enrichment_context_mismatch"
    ][0]
    duplicate = pl.concat([fallback, fallback.head(1)])
    with pytest.raises(pl.exceptions.ComputeError):
        run_weekly_products_pipeline(
            raw, write_raw(tmp_path, duplicate, "duplicate"), out
        )


def test_location_context_conflict_blocks_filling_and_nullable_name_is_allowed(
    tmp_path,
):
    primary = pl.DataFrame(
        {
            "shop": ["s", "s", "s"],
            "location": ["conflict", "safe", "ambiguous"],
            "location_name": [None] * 3,
            "shop_type": [None] * 3,
            "geolocation_latitude": [50.0, None, None],
            "geolocation_longitude": [4.0, None, None],
            "locality": [None] * 3,
            "postcode": [None] * 3,
            "state": [None] * 3,
        }
    )
    fallback = pl.DataFrame(
        {
            "shop": ["s"] * 4,
            "id": ["conflict", "safe", "ambiguous", "ambiguous"],
            "country_code": ["se", "be", "be", "be"],
            "postcode": ["9999", "1000", "1", "2"],
            "geolocation_latitude": [59.0, 50.0, 1.0, 2.0],
            "geolocation_longitude": [14.0, 4.0, 1.0, 2.0],
            "is_enrichment_safe": [True, True, False, False],
        }
    )
    out = tmp_path / "clean/silver_weekly_locations.parquet"
    run_weekly_locations_pipeline(
        write_raw(tmp_path, primary), write_raw(tmp_path, fallback, "fallback"), out
    )
    rows = {r["location"]: r for r in pl.read_parquet(out).to_dicts()}
    assert (
        rows["conflict"]["postcode"] is None
        and rows["conflict"]["country_code"] is None
    )
    assert rows["conflict"]["dq_enrichment_context_mismatch"]
    assert (
        rows["safe"]["postcode"] == "1000" and rows["safe"]["is_coordinates_enriched"]
    )
    assert rows["ambiguous"]["postcode"] is None


def test_nutrition_selection_preserves_portion_and_exposes_hash_ties(tmp_path):
    def payload(nutrients, unit="g"):
        return json.dumps(
            {"nutrients": nutrients, "portion": {"value": 100, "unit": unit}}
        )

    fat = lambda v: {"fats": {"value": v, "unit": "g"}}
    richer = {**fat(2), "salt": {"value": 1, "unit": "g"}}
    data = pl.DataFrame(
        {
            "daltix_id": ["tie", "tie", "rich", "rich", "odd"],
            "shop": ["s"] * 5,
            "country": ["be"] * 5,
            "language": ["nl"] * 5,
            "download_date": [date(2020, 1, 1)] * 5,
            "nutritional_values_std": [
                payload(fat(1), "ml"),
                payload(fat(2), "ml"),
                payload(fat(1)),
                payload(richer),
                payload({"energy": {"value": 1, "unit": "g"}}),
            ],
        }
    )
    out = tmp_path / "clean/silver_nutritionals.parquet"
    metrics = run_nutritionals_pipeline(write_raw(tmp_path, data), out)
    result = pl.read_parquet(out).sort("daltix_id", "nutrient_name")
    assert metrics["hash_tiebreak_grains"] == 1 and metrics["unexpected_unit_rows"] == 1
    assert result.filter(pl.col("daltix_id") == "tie")["portion_unit"][0] == "ml"
    assert result.filter(pl.col("daltix_id") == "rich").height == 2
    run_nutritionals_pipeline(write_raw(tmp_path, data.reverse(), "reversed"), out)
    assert result.equals(pl.read_parquet(out).sort("daltix_id", "nutrient_name"))
    for value in [
        "not JSON",
        '{"nutrients": {}}',
        '{"nutrients": null}',
        '{"nutrients": []}',
    ]:
        bad = data.head(1).with_columns(pl.lit(value).alias("nutritional_values_std"))
        with pytest.raises(RuntimeError, match="contract failed"):
            run_nutritionals_pipeline(write_raw(tmp_path, bad, "invalid"), out)
