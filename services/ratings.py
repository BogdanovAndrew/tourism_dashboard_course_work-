from __future__ import annotations

from typing import Dict, List

from db import execute_query, fetch_all_dicts


def list_attractions() -> List[Dict]:
    return fetch_all_dicts(
        """
        SELECT place_id, place_name, city, category
        FROM tourism_attractions
        ORDER BY place_name
        """
    )


def upsert_rating(user_id: int, place_id: int, rating: float) -> int:
    query = """
    INSERT INTO ratings (user_id, place_id, rating, rated_at)
    VALUES (%s, %s, %s, CURDATE())
    ON DUPLICATE KEY UPDATE
        rating = VALUES(rating),
        rated_at = VALUES(rated_at)
    """
    return execute_query(query, (user_id, place_id, rating))


def delete_rating(user_id: int, place_id: int) -> int:
    return execute_query(
        "DELETE FROM ratings WHERE user_id = %s AND place_id = %s",
        (user_id, place_id),
    )

