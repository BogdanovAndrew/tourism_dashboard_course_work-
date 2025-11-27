from __future__ import annotations

from decimal import Decimal
from typing import Dict, Optional, Tuple, List

import pandas as pd

from db import fetch_all_dicts


def get_available_cities() -> List[str]:
    rows = fetch_all_dicts(
        "SELECT DISTINCT City AS city FROM tourism_packages WHERE City IS NOT NULL ORDER BY City"
    )
    return [row["city"] for row in rows if row.get("city")]


def get_available_categories() -> List[str]:
    rows = fetch_all_dicts(
        "SELECT DISTINCT category FROM tourism_attractions WHERE category IS NOT NULL ORDER BY category"
    )
    return [row["category"] for row in rows if row.get("category")]


def search_packages(
    city: Optional[str],
    category: Optional[str],
    price_range: Tuple[Optional[float], Optional[float]],
    preference_vector: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    params = []
    filters = []
    if city:
        filters.append("tp.City = %s")
        params.append(city)

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    rows = fetch_all_dicts(
        f"""
        SELECT
            tp.Package_id AS package_id,
            tp.City AS city,
            GROUP_CONCAT(ta.place_name ORDER BY seq.idx SEPARATOR ', ') AS itinerary,
            GROUP_CONCAT(DISTINCT ta.category) AS categories,
            SUM(ta.price) AS total_price,
            AVG(ta.overall_rating) AS avg_rating,
            COUNT(ta.place_id) AS stops
        FROM tourism_packages tp
        LEFT JOIN (
            SELECT 1 AS idx UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
        ) AS seq ON 1=1
        LEFT JOIN tourism_attractions ta
            ON ta.place_id = CASE seq.idx
                WHEN 1 THEN tp.Place_Tourism1_id
                WHEN 2 THEN tp.Place_Tourism2_id
                WHEN 3 THEN tp.Place_Tourism3_id
                WHEN 4 THEN tp.Place_Tourism4_id
                WHEN 5 THEN tp.Place_Tourism5_id
            END
        {where_clause}
        GROUP BY tp.Package_id, tp.City
        """,
        tuple(params),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if category:
        df = df[df["categories"].fillna("").str.contains(category, case=False, na=False)]
        if df.empty:
            return df

    min_price, max_price = price_range
    if min_price is not None:
        df = df[df["total_price"] >= min_price]
    if max_price is not None:
        df = df[df["total_price"] <= max_price]
    if df.empty:
        return df

    city_weights = preference_vector.get("city_preference", {})
    category_weights = preference_vector.get("category_preference", {})
    price_weights = preference_vector.get("price_preference", {})

    def price_bucket(total_price: Optional[float]) -> str:
        if total_price is None:
            return "unknown"
        if total_price < 50000:
            return "low"
        if total_price <= 150000:
            return "medium"
        return "high"

    def to_float(value, default=0.0):
        if value is None:
            return default
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def score_row(row):
        city_bonus = to_float(city_weights.get(row["city"], 0.2), 0.2)
        avg_rating = to_float(row.get("avg_rating"))
        total_price = to_float(row.get("total_price"))
        price_component = to_float(price_weights.get(price_bucket(total_price), 0))
        category_component = 0.0
        if category_weights and row.get("categories"):
            cats = [c.strip() for c in row["categories"].split(",") if c]
            if cats:
                category_component = max(
                    to_float(category_weights.get(cat, 0)) for cat in cats
                )
        return round(
            avg_rating * 0.6 + city_bonus * 2 + price_component + category_component * 0.5,
            3,
        )

    scores = df.apply(score_row, axis=1)
    scores = pd.to_numeric(scores, errors="coerce").fillna(0)
    df = df.assign(ranking_score=scores.values)
    df.sort_values("ranking_score", ascending=False, inplace=True)
    return df

