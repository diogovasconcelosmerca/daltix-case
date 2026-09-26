"""Regression cases for missingness, parsing and input protection."""

from datetime import date

import polars as pl
import pytest

from daltix_case.quality.checks import (
    normalize_text,
    protect_inputs,
    require_columns,
    require_date,
    require_finite,
)


def test_missing_tokens_preserve_real_brand_and_codes():
    raw = pl.DataFrame({"brand": [" NAN ", "na", "#N/A", "  ", None, "N/A", "Brand"]})
    assert raw.select(normalize_text("brand"))["brand"].to_list() == [
        "NAN",
        "na",
        None,
        None,
        None,
        None,
        "Brand",
    ]
    assert raw.select(normalize_text("brand", null_tokens=("",)))["brand"][2] == "#N/A"


def test_contracts_reject_nonfinite_schema_drift_and_date_truncation(tmp_path):
    for value in [float("nan"), float("inf"), -float("inf")]:
        with pytest.raises(ValueError, match="finite"):
            require_finite(pl.DataFrame({"price": [value]}), ["price"])
    require_finite(pl.DataFrame({"price": [None, 0.009]}), ["price"])
    with pytest.raises(ValueError, match="unexpected"):
        require_columns(pl.DataFrame({"id": [1], "new": [2]}), ["id"])
    with pytest.raises(ValueError, match="Date"):
        require_date(
            pl.DataFrame({"day": [date(2020, 1, 1)]}).with_columns(
                pl.col("day").cast(pl.Datetime)
            ),
            "day",
        )
    with pytest.raises(ValueError, match="protected"):
        protect_inputs(tmp_path / "raw/a.parquet", tmp_path / "raw/b.parquet")
