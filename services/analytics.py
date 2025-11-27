from __future__ import annotations

import pandas as pd

from db import fetch_all_dicts, fetch_one_dict


def get_popular_places(limit: int = 10) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            ta.place_name,
            ta.city,
            COUNT(r.rating) AS rating_count,
            AVG(r.rating) AS avg_user_rating,
            ta.overall_rating
        FROM tourism_attractions ta
        LEFT JOIN ratings r ON r.place_id = ta.place_id
        GROUP BY ta.place_id
        ORDER BY rating_count DESC, avg_user_rating DESC
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(rows)


def get_city_demand() -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT city, COUNT(*) AS attractions, AVG(overall_rating) AS avg_rating
        FROM tourism_attractions
        GROUP BY city
        ORDER BY attractions DESC
        """
    )
    return pd.DataFrame(rows)


def get_category_satisfaction() -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT category, COUNT(*) AS cnt, AVG(overall_rating) AS avg_rating
        FROM tourism_attractions
        GROUP BY category
        ORDER BY avg_rating DESC
        """
    )
    return pd.DataFrame(rows)


def get_price_segments() -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            CASE
                WHEN price < 50000 THEN 'Бюджет'
                WHEN price BETWEEN 50000 AND 150000 THEN 'Средний'
                ELSE 'Премиум'
            END AS price_segment,
            COUNT(*) AS attractions,
            AVG(overall_rating) AS avg_rating,
            AVG(price) AS avg_price
        FROM tourism_attractions
        GROUP BY price_segment
        ORDER BY avg_price
        """
    )
    return pd.DataFrame(rows)


def get_ratings_timeline(limit: int = 30) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            DATE(rated_at) AS rated_date,
            AVG(rating) AS avg_rating,
            COUNT(*) AS rating_count
        FROM ratings
        WHERE rated_at IS NOT NULL
        GROUP BY rated_date
        ORDER BY rated_date DESC
        LIMIT %s
        """,
        (limit,),
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values("rated_date", inplace=True)
    return df


def get_entity_counts() -> dict:
    row = fetch_one_dict(
        """
        SELECT
            (SELECT COUNT(*) FROM users) AS users_count,
            (SELECT COUNT(*) FROM tourism_attractions) AS attractions_count,
            (SELECT COUNT(*) FROM tourism_packages) AS packages_count,
            (SELECT COUNT(*) FROM ratings) AS ratings_count
        """
    )
    return row or {}


def get_users_overview(limit: int = 100) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            u.user_id,
            u.location,
            u.age,
            COUNT(r.rating) AS rating_count,
            AVG(r.rating) AS avg_user_rating
        FROM users u
        LEFT JOIN ratings r ON r.user_id = u.user_id
        GROUP BY u.user_id, u.location, u.age
        ORDER BY rating_count DESC
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(rows)


def get_recent_ratings(limit: int = 50) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            r.user_id,
            u.location,
            r.place_id,
            ta.place_name,
            ta.city,
            r.rating,
            r.rated_at
        FROM ratings r
        LEFT JOIN users u ON u.user_id = r.user_id
        LEFT JOIN tourism_attractions ta ON ta.place_id = r.place_id
        ORDER BY r.rated_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(rows)


def get_ratings_by_category() -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            ta.category,
            COUNT(*) AS rating_count,
            AVG(r.rating) AS avg_user_rating
        FROM ratings r
        JOIN tourism_attractions ta ON ta.place_id = r.place_id
        GROUP BY ta.category
        ORDER BY rating_count DESC
        """
    )
    return pd.DataFrame(rows)


def get_ratings_by_city() -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            ta.city,
            COUNT(*) AS rating_count,
            AVG(r.rating) AS avg_user_rating
        FROM ratings r
        JOIN tourism_attractions ta ON ta.place_id = r.place_id
        GROUP BY ta.city
        ORDER BY rating_count DESC
        """
    )
    return pd.DataFrame(rows)


def get_user_activity(limit: int = 20) -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            u.user_id,
            u.location,
            COUNT(r.rating) AS rating_count,
            AVG(r.rating) AS avg_user_rating
        FROM users u
        LEFT JOIN ratings r ON r.user_id = u.user_id
        GROUP BY u.user_id, u.location
        ORDER BY rating_count DESC
        LIMIT %s
        """,
        (limit,),
    )
    return pd.DataFrame(rows)


def get_package_coverage() -> pd.DataFrame:
    rows = fetch_all_dicts(
        """
        SELECT
            City AS city,
            COUNT(*) AS package_count,
            SUM(
                (Place_Tourism1_id IS NOT NULL) +
                (Place_Tourism2_id IS NOT NULL) +
                (Place_Tourism3_id IS NOT NULL) +
                (Place_Tourism4_id IS NOT NULL) +
                (Place_Tourism5_id IS NOT NULL)
            ) AS total_stops
        FROM tourism_packages
        GROUP BY City
        ORDER BY package_count DESC
        """
    )
    return pd.DataFrame(rows)

