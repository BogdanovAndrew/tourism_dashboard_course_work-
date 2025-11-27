from __future__ import annotations

import hashlib
from typing import Dict, Optional

from db import column_names, fetch_one_dict


def _derive_candidate_hashes(raw_password: str) -> Dict[str, str]:
    sha256 = hashlib.sha256(raw_password.encode("utf-8")).hexdigest()
    sha1 = hashlib.sha1(raw_password.encode("utf-8")).hexdigest()
    md5 = hashlib.md5(raw_password.encode("utf-8")).hexdigest()
    return {"sha256": sha256, "sha1": sha1, "md5": md5}


BLOCK_COLUMN_CANDIDATES = ("is_blocked", "blocked")


def _resolve_login_column() -> Optional[str]:
    cols = column_names("users_credentials")
    for candidate in ("login", "username", "user_name", "email", "user_email"):
        if candidate in cols:
            return candidate
    return None


def _resolve_password_column(cols: set) -> Optional[str]:
    for candidate in ("password_value", "password_hash", "password", "pwd_hash", "pwd"):
        if candidate in cols:
            return candidate
    return None


def _resolve_block_column(cols: set) -> Optional[str]:
    for candidate in BLOCK_COLUMN_CANDIDATES:
        if candidate in cols:
            return candidate
    return None


def authenticate(username: str, password: str) -> Optional[Dict]:
    """Возвращает словарь с user_id и username при успешной авторизации."""
    login_column = _resolve_login_column()
    if not login_column:
        return None

    record = fetch_one_dict(
        f"SELECT * FROM users_credentials WHERE {login_column} = %s LIMIT 1",
        (username,),
    )
    if not record:
        return None

    cols = set(column_names("users_credentials"))
    password_column = _resolve_password_column(cols)
    stored_password = record.get(password_column) if password_column else None
    if stored_password is None:
        return None

    block_column = _resolve_block_column(cols)
    if block_column:
        block_value = record.get(block_column)
        if block_value is not None and str(block_value).lower() in {"1", "true", "yes", "blocked"}:
            raise PermissionError("Пользователь заблокирован администратором")

    if stored_password == password:
        return {"user_id": record.get("user_id"), "username": username}

    derived = _derive_candidate_hashes(password)
    if stored_password in derived.values():
        return {"user_id": record.get("user_id"), "username": username}

    return None

