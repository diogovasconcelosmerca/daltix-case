"""Daltix case: source discovery and reproducible Silver transformations."""


def main() -> None:
    """Keep the installed console entry point compatible with the package."""
    from daltix_case.pipelines.silver_pipeline import main as silver_main

    silver_main()
