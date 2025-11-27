from __future__ import annotations

import json
from typing import Dict

import pandas as pd

from db import call_procedure, fetch_all_dicts


def fetch_function_recommendations(user_id: int, limit: int = 15) -> pd.DataFrame:
    """Получает скоринговые рекомендации через функцию get_recommendation_score."""
    query = """
        SELECT
            ta.place_id,
            ta.place_name,
            ta.category,
            ta.city,
            ta.price,
            ta.overall_rating,
            get_recommendation_score(%s, ta.place_id) AS recommendation_score
        FROM tourism_attractions ta
        ORDER BY recommendation_score DESC
        LIMIT %s
    """
    try:
        rows = fetch_all_dicts(query, (user_id, limit))
    except Exception:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if not df.empty:
        df["source"] = "db_function_score"
    return df


def fetch_db_recommendations(user_id: int) -> pd.DataFrame:
    """Обратная совместимость: попытка вызвать процедуру/функцию get_recommendations."""
    try:
        result = call_procedure("get_recommendations", [user_id])
    except Exception:
        try:
            rows = fetch_all_dicts("SELECT get_recommendations(%s) AS payload", (user_id,))
        except Exception:
            return pd.DataFrame()
        if rows and rows[0].get("payload"):
            payload = rows[0]["payload"]
            if isinstance(payload, str):
                try:
                    parsed = json.loads(payload)
                    return pd.DataFrame(parsed)
                except Exception:
                    return pd.DataFrame(
                        [{"recommendation": payload, "source": "db_function"}]
                    )
        return pd.DataFrame()
    return pd.DataFrame(result)


def _compute_category_scores(ratings_df: pd.DataFrame) -> Dict[str, float]:
    if ratings_df.empty:
        return {}
    category_group = ratings_df.groupby("category")["rating"].mean()
    normalized = category_group / category_group.max()
    return normalized.to_dict()


def build_fallback_recommendations(
    user_id: int, preference_vector: Dict[str, Dict[str, float]]
) -> pd.DataFrame:
    past_ratings = fetch_all_dicts(
        """
        SELECT r.rating, ta.category, ta.place_id, ta.place_name, ta.city,
               ta.overall_rating, ta.price
        FROM ratings r
        JOIN tourism_attractions ta ON ta.place_id = r.place_id
        WHERE r.user_id = %s
        """,
        (user_id,),
    )
    ratings_df = pd.DataFrame(past_ratings)
    cat_scores = (
        preference_vector.get("category_preference")
        or preference_vector.get("category")
        or _compute_category_scores(ratings_df)
    )

    attractions = pd.DataFrame(
        fetch_all_dicts(
            """
            SELECT place_id, place_name, category, city, price, overall_rating
            FROM tourism_attractions
            """
        )
    )
    if attractions.empty:
        return pd.DataFrame()

    def score_row(row):
        category_weight = cat_scores.get(row["category"], 0.3)
        base = row.get("overall_rating") or 0
        return round(base * 0.6 + category_weight * 4, 3)

    attractions["score"] = attractions.apply(score_row, axis=1)
    attractions.sort_values(["score", "overall_rating"], ascending=False, inplace=True)
    attractions["source"] = "python_fallback"
    return attractions.head(10)


def get_recommendations(user_id: int, preference_vector: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    function_df = fetch_function_recommendations(user_id)
    if not function_df.empty:
        return function_df

    db_df = fetch_db_recommendations(user_id)
    if not db_df.empty:
        db_df["source"] = db_df.get("source", "db_procedure")
        return db_df

    return build_fallback_recommendations(user_id, preference_vector)

