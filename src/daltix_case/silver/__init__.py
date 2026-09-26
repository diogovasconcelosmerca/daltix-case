"""Silver table builders and their published file names."""

SILVER_FILES = {
    name: f"silver_{name}.parquet"
    for name in (
        "products",
        "locations",
        "prices",
        "nutritionals",
        "weekly_products",
        "weekly_locations",
        "weekly_prices",
    )
}
