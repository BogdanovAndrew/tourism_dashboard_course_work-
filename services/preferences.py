from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd

from db import fetch_all_dicts, fetch_one_dict


def get_user_profile(user_id: int) -> Optional[Dict]:
    return fetch_one_dict(
        "SELECT user_id, location, age FROM users WHERE user_id = %s",
        (user_id,),
    )


def get_user_preferences(user_id: int) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT preference_type, preference_key, preference_value
        FROM user_preferences
        WHERE user_id = %s
        ORDER BY preference_type, preference_key
        """,
        (user_id,),
    )
    return pd.DataFrame(rows)


def get_user_ratings(user_id: int) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            r.place_id,
            ta.place_name,
            ta.category,
            ta.city,
            ta.price,
            ta.overall_rating,
            r.rating,
            r.rated_at
        FROM ratings r
        JOIN tourism_attractions ta ON ta.place_id = r.place_id
        WHERE r.user_id = %s
        ORDER BY r.rated_at DESC
        """,
        (user_id,),
    )
    return pd.DataFrame(rows)


def build_preference_vector(pref_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    vector: Dict[str, Dict[str, float]] = {}
    if pref_df.empty:
        return vector
    grouped = pref_df.groupby("preference_type")
    for pref_type, slice_df in grouped:
        mapping = {
            str(row.preference_key): float(row.preference_value)
            for row in slice_df.itertuples(index=False)
        }
        vector[pref_type] = mapping
    return vector

