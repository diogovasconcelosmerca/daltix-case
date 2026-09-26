"""Offline end-to-end and failure-publication tests with seven tiny sources."""

import json
from datetime import date

import polars as pl
import pytest

from daltix_case.pipelines import silver_pipeline as pipeline
from daltix_case.source_io import sha256_file, validate_local_snapshot


def snapshot(raw):
    raw.mkdir()
    product = pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["s"],
            "name": ["NAN milk"],
            "brand": ["nan"],
            "country": ["be"],
            "language": ["nl"],
            "description": [None],
            "categories": [None],
        }
    )
    product.write_parquet(raw / "products.parquet")
    product.write_parquet(raw / "weekly_prices_products.parquet")
    pl.DataFrame(
        {
            "shop": ["s"],
            "id": ["l"],
            "country_code": ["be"],
            "type": [None],
            "postcode": ["1000"],
            "geolocation_latitude": [50.0],
            "geolocation_longitude": [4.0],
            "sources": ['["online"]'],
        }
    ).write_parquet(raw / "locations.parquet")
    pl.DataFrame(
        {
            "shop": ["s"],
            "location": ["l"],
            "location_name": [None],
            "shop_type": [None],
            "postcode": [None],
            "locality": [None],
            "state": [None],
            "geolocation_latitude": [None],
            "geolocation_longitude": [None],
        }
    ).write_parquet(raw / "weekly_prices_locations.parquet")
    pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["s"],
            "country": ["be"],
            "location": ["l"],
            "price": [1.0],
            "promo_price": [None],
            "downloaded_on": [date(2020, 1, 6)],
            "unit_std": ["su"],
            "currency": ["eur"],
        }
    ).write_parquet(raw / "prices.parquet")
    pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["s"],
            "location": ["l"],
            "week": [date(2020, 1, 6)],
            "price": [1.0],
            "price_promo": [1.0],
        }
    ).write_parquet(raw / "weekly_prices.parquet")
    pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["s"],
            "country": ["be"],
            "language": ["nl"],
            "download_date": [date(2020, 1, 6)],
            "nutritional_values_std": [
                json.dumps(
                    {
                        "nutrients": {"fats": {"value": 1, "unit": "g"}},
                        "portion": {"value": 100, "unit": "ml"},
                    }
                )
            ],
        }
    ).write_parquet(raw / "nutritionals.parquet")
    return validate_local_snapshot(raw)


def test_full_pipeline_and_failed_rebuild_preserve_last_complete_checkpoint(
    tmp_path, monkeypatch
):
    raw, clean = tmp_path / "raw", tmp_path / "clean"
    original = snapshot(raw)
    result = pipeline.run_silver_pipeline(raw, clean)
    assert result["weekly_locations"]["postcodes_enriched"] == 1
    assert result["products"]["missing_brand"] == 0
    manifest = pipeline.load_silver_manifest(clean)
    assert manifest["metrics"] == result and len(manifest["files"]) == 7
    assert {p.name for p in clean.glob("*.parquet")} == set(
        pipeline.SILVER_FILES.values()
    )
    assert all(f["file"] == f"silver_{f['dataset']}.parquet" for f in manifest["files"])
    before = {p.name: sha256_file(p) for p in clean.iterdir() if p.is_file()}

    def fail(**kwargs):
        raise RuntimeError("Injected final-table failure")

    monkeypatch.setattr(pipeline, "run_weekly_prices_pipeline", fail)
    with pytest.raises(RuntimeError, match="Injected"):
        pipeline.run_silver_pipeline(raw, clean)
    assert before == {p.name: sha256_file(p) for p in clean.iterdir() if p.is_file()}
    assert original == validate_local_snapshot(raw)
    assert pipeline.load_silver_manifest(clean) == manifest
    assert not list(clean.glob(".silver-build-*"))


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_publication_rolls_back_or_preserves_recovery_evidence(
    tmp_path, monkeypatch, rollback_fails
):
    staging, target = tmp_path / "staging", tmp_path / "target"
    staging.mkdir()
    target.mkdir()
    names = list(pipeline.SILVER_FILES.values()) + ["manifest.json"]
    for name in names:
        (staging / name).write_text("new")
        (target / name).write_text("old")
    original_replace = pipeline.os.replace

    def replace(src, dst):
        src = Path(src)
        if src.parent == staging and src.name == "silver_prices.parquet":
            raise OSError("Injected publication failure")
        if rollback_fails and src.parent.name == ".previous":
            raise OSError("Injected rollback failure")
        return original_replace(src, dst)

    from pathlib import Path

    monkeypatch.setattr(pipeline.os, "replace", replace)
    expected = pipeline.PublicationRecoveryError if rollback_fails else OSError
    with pytest.raises(expected):
        pipeline._publish(staging, target)
    if rollback_fails:
        assert (staging / ".previous/silver_products.parquet").read_text() == "old"
    else:
        assert all((target / name).read_text() == "old" for name in names)
