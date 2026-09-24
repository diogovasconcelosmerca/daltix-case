"""Explicit source access and local snapshot checks for the discovery notebook."""

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import psycopg
import pyarrow.parquet as pq
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

TABLES = (
    "locations",
    "nutritionals",
    "prices",
    "products",
    "weekly_prices",
    "weekly_prices_locations",
    "weekly_prices_products",
)


def utc_now():
    return datetime.now(UTC).isoformat()


def source_config():
    """Read configuration only when source access is explicitly requested."""
    names = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SCHEMA")
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise ValueError("Missing source configuration: " + ", ".join(missing))
    return {
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "schema": os.environ["DB_SCHEMA"],
    }


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def connection_info(config):
    # libpq escaping and SQL literal escaping are separate operations.
    return make_conninfo(
        **{key: config[key] for key in ("host", "port", "dbname", "user", "password")},
        connect_timeout=10,
        options="-c default_transaction_read_only=on -c statement_timeout=30000",
    )


@contextmanager
def source_connection():
    config = source_config()
    with psycopg.connect(
        connection_info(config), autocommit=True, row_factory=dict_row
    ) as conn:
        yield conn


@contextmanager
def attached_source(config):
    """Use a separate short-lived connection for source extraction only."""
    with duckdb.connect() as connection:
        connection.execute("INSTALL postgres")
        connection.execute("LOAD postgres")
        # This string contains credentials: never print or persist it.
        connection.execute(
            f"ATTACH {quote_literal(connection_info(config))} AS pg_source "
            f"(TYPE postgres, READ_ONLY, SCHEMA {quote_literal(config['schema'])})"
        )
        yield connection


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_files(raw_dir):
    """Read Parquet metadata and fingerprints; do not query PostgreSQL."""
    records = []
    for table in TABLES:
        path = Path(raw_dir) / f"{table}.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"Missing raw file: {path.name}")
        parquet = pq.ParquetFile(path)
        try:
            records.append(
                {
                    "table": table,
                    "file": path.name,
                    "rows": parquet.metadata.num_rows,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "schema": [
                        {
                            "name": field.name,
                            "type": str(field.type),
                            "nullable": field.nullable,
                        }
                        for field in parquet.schema_arrow
                    ],
                }
            )
        finally:
            parquet.close()
    return records


def manifest_document(records, provenance):
    return {
        "manifest_version": 1,
        "recorded_at_utc": utc_now(),
        "provenance": provenance,
        "transactionally_consistent_across_tables": False,
        "files": records,
    }


def validate_local_snapshot(raw_dir):
    """Create an honest baseline for existing files or validate it on rerun."""
    raw_dir = Path(raw_dir)
    records = inspect_files(raw_dir)
    manifest_path = raw_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("manifest_version") != 1 or manifest.get("files") != records:
            raise ValueError(
                "Raw files differ from the recorded manifest; investigate before proceeding."
            )
        return manifest
    manifest = manifest_document(
        records,
        {
            "origin": "existing_local_files",
            "source_system": "PostgreSQL, as documented in the discovery notebook",
            "source_extracted_at_utc": None,
            "source_row_reconciliation": "unavailable_for_historical_extraction",
            "note": "Baseline recorded during checkpoint preparation; original extraction time and source state are not inferred.",
        },
    )
    with manifest_path.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    return manifest


def extract_snapshot(raw_dir, *, enabled=False):
    """Export to staging, reconcile COPY counts, then publish without overwrite."""
    if not enabled:
        return None
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    destinations = [raw_dir / f"{table}.parquet" for table in TABLES]
    destinations.append(raw_dir / "manifest.json")
    if any(path.exists() for path in destinations):
        raise FileExistsError(
            "Raw snapshot already exists; choose a separate empty destination."
        )
    config = source_config()
    started = utc_now()
    with tempfile.TemporaryDirectory(prefix=".extract-", dir=raw_dir) as directory:
        staging = Path(directory)
        exported_counts = {}
        with attached_source(config) as connection:
            for table in TABLES:
                source = ".".join(
                    quote_identifier(part)
                    for part in ("pg_source", config["schema"], table)
                )
                result = connection.execute(
                    f"COPY {source} TO {quote_literal(staging / f'{table}.parquet')} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)"
                ).fetchone()
                exported_counts[table] = result[0]
        records = inspect_files(staging)
        if any(
            record["rows"] != exported_counts[record["table"]] for record in records
        ):
            raise ValueError(
                "Export row count does not match Parquet metadata; snapshot not published."
            )
        manifest = manifest_document(
            records,
            {
                "origin": "source_export",
                "source_schema": config["schema"],
                "source_extraction_started_at_utc": started,
                "source_extraction_completed_at_utc": utc_now(),
                "source_row_reconciliation": "COPY returned rows equal local Parquet rows; no separate source COUNT scan",
                "exported_rows": exported_counts,
            },
        )
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        # Hard links fail if the target exists; keep staging on the same filesystem.
        # Publish the manifest last. Readers require it and the complete file set.
        published = []
        try:
            for name in [f"{table}.parquet" for table in TABLES] + ["manifest.json"]:
                destination = raw_dir / name
                os.link(staging / name, destination)
                published.append(destination)
        except Exception:
            for destination in reversed(published):
                destination.unlink()
            raise
    return manifest
