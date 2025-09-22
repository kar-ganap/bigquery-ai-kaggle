"""
SQL generation helpers for safe BigQuery query construction
"""

def safe_sql_string_list(brands: list) -> str:
    """
    Generate a safe SQL IN clause string from a list of brands.

    Args:
        brands: List of brand names (strings)

    Returns:
        Comma-separated quoted string for SQL IN clause

    Example:
        safe_sql_string_list(['Warby Parker', "Ray's Eyewear"])
        → "'Warby Parker', 'Ray''s Eyewear'"
    """
    if not brands:
        return "'__EMPTY__'"  # Fallback to prevent empty IN clause

    # Escape single quotes by doubling them (SQL standard)
    escaped_brands = [brand.replace("'", "''") for brand in brands]

    # Quote each brand and join with commas
    quoted_brands = [f"'{brand}'" for brand in escaped_brands]

    return ', '.join(quoted_brands)

def safe_brand_in_clause(primary_brand: str, competitor_brands: list = None) -> str:
    """
    Generate a safe SQL IN clause for brand filtering.

    Args:
        primary_brand: The main brand name
        competitor_brands: List of competitor brand names (optional)

    Returns:
        Complete IN clause string for SQL WHERE conditions

    Example:
        safe_brand_in_clause('Warby Parker', ['LensCrafters', 'Zenni'])
        → "('Warby Parker', 'LensCrafters', 'Zenni')"
    """
    all_brands = [primary_brand]
    if competitor_brands:
        all_brands.extend(competitor_brands)

    return f"({safe_sql_string_list(all_brands)})"