from __future__ import annotations

from typing import List, Tuple

import pandas as pd

from db import column_names, execute_query, fetch_all_dicts

BLOCK_COLUMN_CANDIDATES = ("is_blocked", "blocked")


def _resolve_block_column() -> str | None:
    cols = column_names("users_credentials")
    for candidate in BLOCK_COLUMN_CANDIDATES:
        if candidate in cols:
            return candidate
    return None


def get_credentials_overview() -> Tuple[pd.DataFrame, bool]:
    block_col = _resolve_block_column()
    select = "SELECT user_id, login"
    if block_col:
        select += f", {block_col} AS is_blocked"
    else:
        select += ", NULL AS is_blocked"
    select += " FROM users_credentials ORDER BY login"
    rows = fetch_all_dicts(select)
    return pd.DataFrame(rows), block_col is not None


def set_user_block_status(user_id: int, blocked: bool):
    block_col = _resolve_block_column()
    if not block_col:
        raise RuntimeError("Колонка блокировки отсутствует в таблице users_credentials.")
    execute_query(
        f"UPDATE users_credentials SET {block_col} = %s WHERE user_id = %s",
        (1 if blocked else 0, user_id),
    )



