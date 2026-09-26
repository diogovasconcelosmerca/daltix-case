"""Small, explicit cleaning rules and structural contracts."""

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Assessed tokens only. NAN is a real brand; NA can be a legitimate code.
SEMANTIC_NULLS = ("", "null", "none", "n/a", "#n/a")


def normalize_text(
    column: str, *, null_tokens: tuple[str, ...] = SEMANTIC_NULLS
) -> pl.Expr:
    """Trim text and normalize only the explicitly accepted missing tokens."""
    value = pl.col(column).cast(pl.String).str.strip_chars()
    return (
        pl.when(value.is_null() | value.str.to_lowercase().is_in(null_tokens))
        .then(None)
        .otherwise(value)
        .alias(column)
    )


def require_columns(
    df: pl.DataFrame, required: list[str], *, exact: bool = True
) -> None:
    """Reject unreviewed schema changes instead of silently dropping new fields."""
    missing = sorted(set(required) - set(df.columns))
    unexpected = sorted(set(df.columns) - set(required)) if exact else []
    if missing or unexpected:
        raise ValueError(
            f"Source schema changed: missing={missing}, unexpected={unexpected}"
        )


def require_unique(df: pl.DataFrame, key: list[str], name: str) -> None:
    """Validate uniqueness; key completeness is a separate contract."""
    groups = df.group_by(key).len().filter(pl.col("len") > 1).height
    if groups:
        raise ValueError(f"{name}: {groups} duplicated business-key groups found.")


def require_date(df: pl.DataFrame, column: str) -> None:
    """Do not silently truncate a new timestamp schema to dates."""
    if df.schema[column] != pl.Date:
        raise ValueError(
            f"{column}: expected Date, got {df.schema[column]}; review source drift."
        )


def require_finite(df: pl.DataFrame, columns: list[str]) -> None:
    """Allow genuine NULLs, but reject NaN and infinity in populated numbers."""
    for column in columns:
        if df.filter(pl.col(column).is_not_null() & ~pl.col(column).is_finite()).height:
            raise ValueError(f"{column}: populated numeric values must be finite.")


def protect_inputs(raw_path: Path, output_path: Path, *other_inputs: Path) -> None:
    """Never overwrite Raw or a fallback input, including through path aliases."""
    raw, output = raw_path.resolve(), output_path.resolve()
    if output.is_relative_to(raw.parent) or output in {
        p.resolve() for p in other_inputs
    }:
        raise ValueError("Silver destination overlaps a protected input.")


def fail_if_false(checks: dict[str, bool], pipeline_name: str) -> None:
    """Stop on structural violations; log successful validation."""
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"{pipeline_name} contract failed: {failed}")
    logger.info("%s contract passed (%d checks)", pipeline_name, len(checks))
