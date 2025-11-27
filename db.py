from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence

import mysql.connector
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool

from config import get_settings


_pool: Optional[MySQLConnectionPool] = None


def _ensure_pool() -> MySQLConnectionPool:
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = MySQLConnectionPool(
            pool_name=settings.mysql_pool_name,
            pool_size=settings.mysql_pool_size,
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_db,
            charset="utf8mb4",
            autocommit=True,
        )
    return _pool


@contextmanager
def get_connection():
    pool = _ensure_pool()
    conn = pool.get_connection()
    try:
        yield conn
    finally:
        conn.close()


def fetch_all_dicts(query: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        rows = cursor.fetchall()
        cursor.close()
    return rows


def fetch_one_dict(query: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        row = cursor.fetchone()
        cursor.close()
    return row


def execute_query(query: str, params: Optional[Sequence[Any]] = None) -> int:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        affected = cursor.rowcount
        cursor.close()
    return affected


def call_procedure(proc_name: str, args: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.callproc(proc_name, args or ())
        result_sets: List[Dict[str, Any]] = []
        for result in cursor.stored_results():
            result_sets.extend(result.fetchall())
        cursor.close()
    return result_sets


def column_names(table_name: str) -> List[str]:
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
    """
    rows = fetch_all_dicts(query, (table_name,))
    return [row["COLUMN_NAME"] for row in rows]

