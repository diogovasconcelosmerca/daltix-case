"""Build, validate and publish one complete Raw-to-Silver checkpoint."""

import argparse
import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from daltix_case.quality.cross_table import run_cross_table_contracts
from daltix_case.silver import SILVER_FILES
from daltix_case.silver.locations import run_locations_pipeline
from daltix_case.silver.nutritionals import run_nutritionals_pipeline
from daltix_case.silver.prices import run_prices_pipeline
from daltix_case.silver.products import run_products_pipeline
from daltix_case.silver.weekly_locations import run_weekly_locations_pipeline
from daltix_case.silver.weekly_prices import run_weekly_prices_pipeline
from daltix_case.silver.weekly_products import run_weekly_products_pipeline
from daltix_case.source_io import sha256_file, validate_local_snapshot

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DIR = PROJECT_ROOT / "data" / "clean"
OUTPUTS = tuple(SILVER_FILES)


def code_fingerprint() -> str:
    """Identify the exact local Python implementation, including uncommitted work."""
    package = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted(package.rglob("*.py")):
        digest.update(path.relative_to(package).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _output_records(directory: Path) -> list[dict]:
    records = []
    for name in OUTPUTS:
        path = directory / SILVER_FILES[name]
        with pq.ParquetFile(path) as parquet:
            records.append(
                {
                    "dataset": name,
                    "file": path.name,
                    "rows": parquet.metadata.num_rows,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "schema": [
                        {"name": f.name, "type": str(f.type)}
                        for f in parquet.schema_arrow
                    ],
                }
            )
    return records


def load_silver_manifest(silver_dir: Path = SILVER_DIR) -> dict:
    """Read only complete, current-code artifacts; never rebuild data implicitly."""
    silver_dir = Path(silver_dir)
    if (silver_dir / ".pipeline.lock").exists():
        raise RuntimeError(
            "Silver build/publication is in progress; wait before reading outputs."
        )
    path = silver_dir / "manifest.json"
    if not path.is_file():
        raise FileNotFoundError(
            "No validated Silver checkpoint. Run the official Silver pipeline first."
        )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("manifest_version") != 1 or manifest.get("status") != "complete":
        raise RuntimeError("Silver manifest is not a completed supported checkpoint.")
    if manifest.get("code_sha256") != code_fingerprint():
        raise RuntimeError(
            "Silver was built with different source code; rebuild before interpreting results."
        )
    if manifest["files"] != _output_records(silver_dir):
        raise RuntimeError(
            "Silver files differ from the completed manifest; rebuild or investigate."
        )
    if (silver_dir / ".pipeline.lock").exists():
        raise RuntimeError(
            "Silver publication started during validation; retry after it finishes."
        )
    return manifest


class PublicationRecoveryError(RuntimeError):
    """Publication and rollback failed; preserve staging, backups and lock."""


def _publish(staging: Path, destination: Path) -> None:
    """Publish validated files with rollback on ordinary failure; manifest goes last.

    Individual replacements are atomic, not the seven-file set. The lock excludes
    cooperative readers. After process/machine failure retain the lock and inspect
    staging/backup before recovery; never advertise the directory as complete.
    """
    names = list(SILVER_FILES.values()) + ["manifest.json"]
    backup = staging / ".previous"
    backup.mkdir()
    moved, published = [], []
    try:
        for name in names:
            target = destination / name
            if target.exists():
                os.replace(target, backup / name)
                moved.append(name)
            os.replace(staging / name, target)
            published.append(name)
    except Exception:
        try:
            for name in reversed(published):
                (destination / name).unlink()
            for name in reversed(moved):
                os.replace(backup / name, destination / name)
        except Exception as recovery_error:
            raise PublicationRecoveryError(
                f"Publication rollback needs manual recovery. Keep {staging} and .pipeline.lock."
            ) from recovery_error
        raise


def run_silver_pipeline(
    raw_dir: Path = RAW_DIR, silver_dir: Path = SILVER_DIR
) -> dict[str, dict]:
    """Build all seven sources in isolation, validate, then publish together."""
    raw_dir, silver_dir = Path(raw_dir).resolve(), Path(silver_dir).resolve()
    if silver_dir.is_relative_to(raw_dir) or raw_dir.is_relative_to(silver_dir):
        raise ValueError(
            "Raw and Silver directories must be separate, non-nested paths."
        )
    if not (raw_dir / "manifest.json").is_file():
        raise FileNotFoundError(
            "Raw manifest is required; establish/validate the snapshot in discovery first."
        )
    raw_manifest = validate_local_snapshot(raw_dir)
    source_hash = code_fingerprint()
    started = datetime.now(UTC).isoformat()
    start = time.perf_counter()
    silver_dir.mkdir(parents=True, exist_ok=True)
    lock = silver_dir / ".pipeline.lock"
    # Exclusive creation prevents concurrent writers and leaves crash evidence.
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(started)
    metrics, timings = {}, {}
    logger.info("Building Silver from %s", raw_dir)
    staging = None
    keep_recovery = False
    try:
        staging = Path(tempfile.mkdtemp(prefix=".silver-build-", dir=silver_dir))
        jobs = [
            (
                "products",
                run_products_pipeline,
                {"raw_path": raw_dir / "products.parquet"},
            ),
            (
                "locations",
                run_locations_pipeline,
                {"raw_path": raw_dir / "locations.parquet"},
            ),
            ("prices", run_prices_pipeline, {"raw_path": raw_dir / "prices.parquet"}),
            (
                "nutritionals",
                run_nutritionals_pipeline,
                {"raw_path": raw_dir / "nutritionals.parquet"},
            ),
            (
                "weekly_products",
                run_weekly_products_pipeline,
                {
                    "raw_path": raw_dir / "weekly_prices_products.parquet",
                    "fallback_path": staging / SILVER_FILES["products"],
                },
            ),
            (
                "weekly_locations",
                run_weekly_locations_pipeline,
                {
                    "raw_path": raw_dir / "weekly_prices_locations.parquet",
                    "fallback_path": staging / SILVER_FILES["locations"],
                },
            ),
            (
                "weekly_prices",
                run_weekly_prices_pipeline,
                {"raw_path": raw_dir / "weekly_prices.parquet"},
            ),
        ]
        for name, builder, inputs in jobs:
            tick = time.perf_counter()
            metrics[name] = builder(**inputs, silver_path=staging / SILVER_FILES[name])
            timings[name] = round(time.perf_counter() - tick, 3)
            logger.info("%s | %.3fs | %s", name, timings[name], metrics[name])
        metrics["cross_table"] = run_cross_table_contracts(staging)
        # Detect concurrent edits to either input snapshot or implementation.
        if (
            validate_local_snapshot(raw_dir) != raw_manifest
            or code_fingerprint() != source_hash
        ):
            raise RuntimeError(
                "Inputs or source code changed during the build; nothing published."
            )
        manifest = {
            "manifest_version": 1,
            "status": "complete",
            "started_at_utc": started,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "code_sha256": source_hash,
            "raw_files": raw_manifest["files"],
            "raw_provenance": raw_manifest["provenance"],
            "versions": {
                "python": platform.python_version(),
                **{
                    name: importlib.metadata.version(name)
                    for name in ["polars", "duckdb", "pyarrow"]
                },
            },
            "metrics": metrics,
            "table_seconds": timings,
            "files": _output_records(staging),
            "publication": "staged_validation_then_replacement_with_lock_and_rollback",
        }
        manifest["build_seconds"] = round(time.perf_counter() - start, 3)
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        _publish(staging, silver_dir)
        logger.info("Silver complete | %.3fs", time.perf_counter() - start)
        return metrics
    except PublicationRecoveryError:
        keep_recovery = True
        raise
    finally:
        if not keep_recovery:
            # Only remove our own verified temporary child of the output directory.
            if staging is not None:
                resolved = staging.resolve()
                if resolved.parent != silver_dir or not resolved.name.startswith(
                    ".silver-build-"
                ):
                    raise RuntimeError(
                        "Refusing cleanup outside the expected staging directory."
                    )
                shutil.rmtree(resolved)
            lock.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and validate the Daltix Silver checkpoint."
    )
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    run_silver_pipeline(args.raw_dir, args.silver_dir)


if __name__ == "__main__":
    main()
