#!/usr/bin/env python3
"""
Ingest WHO GHO API indicator data into local raw files (JSONL).

Writes:
  data/raw/who/<indicator_code>/ingest_date=YYYY-MM-DD/part-00000.jsonl

Why JSONL?
- Append-friendly
- Great for DuckDB read_json_auto()
- Keeps each record intact (bronze/raw layer)

Example:
  python scripts/ingest_who.py --indicator MDG_0000000007
  python scripts/ingest_who.py --indicator MDG_0000000007 --top 5000
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlencode

import requests
import snowflake.connector
from dotenv import load_dotenv


GHO_BASE = "https://ghoapi.azureedge.net/api"


def utc_today_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def project_root_from_this_file() -> Path:
    # scripts/ingest_who.py -> project root is parent of scripts/
    return Path(__file__).resolve().parent.parent


def build_url(
    indicator: str,
    skip: int = 0,
    top: int = 1000,
    select: Optional[str] = None,
    filters: Optional[str] = None,
) -> str:
    """
    WHO GHO OData pagination uses $top and $skip.
    """
    endpoint = f"{GHO_BASE}/{indicator}"

    params = {
        "$format": "json",
        "$top": str(top),
        "$skip": str(skip),
    }
    if select:
        params["$select"] = select
    if filters:
        params["$filter"] = filters

    return endpoint + "?" + urlencode(params)


def iter_pages(
    indicator: str,
    *,
    session: requests.Session,
    page_size: int,
    max_rows: Optional[int],
    select: Optional[str],
    filters: Optional[str],
    sleep_s: float,
    timeout_s: int,
) -> Iterable[Dict[str, Any]]:
    """
    Yields records across paginated responses.
    Stops when API returns empty page or reaches max_rows.
    """
    fetched = 0
    skip = 0

    while True:
        # adjust last page size if max_rows is set
        top = page_size
        if max_rows is not None:
            remaining = max_rows - fetched
            if remaining <= 0:
                return
            top = min(top, remaining)

        url = build_url(indicator, skip=skip, top=top, select=select, filters=filters)
        resp = session.get(url, timeout=timeout_s)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} from WHO API:\n{resp.text[:500]}")

        payload = resp.json()
        rows = payload.get("value", [])
        if not rows:
            return

        for r in rows:
            yield r
        fetched += len(rows)
        skip += len(rows)

        if sleep_s > 0:
            time.sleep(sleep_s)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """
    Writes rows to a JSONL file. Returns number of rows written.
    """
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False))
            f.write("\n")
            n += 1
    return n


def get_snowflake_config() -> Dict[str, str]:
    env_map = {
        "SNOWFLAKE_ACCOUNT": "account",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_PASSWORD": "password",
        "SNOWFLAKE_WAREHOUSE": "warehouse",
        "SNOWFLAKE_DATABASE": "database",
        "SNOWFLAKE_SCHEMA": "schema",
    }
    optional_env_map = {
        "SNOWFLAKE_ROLE": "role",
    }
    config: Dict[str, str] = {}
    missing = []

    for env_key, cfg_key in env_map.items():
        value = os.getenv(env_key)
        if not value:
            missing.append(env_key)
        else:
            config[cfg_key] = value

    for env_key, cfg_key in optional_env_map.items():
        value = os.getenv(env_key)
        if value:
            config[cfg_key] = value

    if missing:
        raise RuntimeError("Missing Snowflake env vars: " + ", ".join(missing))

    return config


def connect_to_snowflake(config: Dict[str, str]):
    return snowflake.connector.connect(**config)


def ensure_snowflake_table(cursor: snowflake.connector.cursor.SnowflakeCursor, table_name: str) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            indicator STRING,
            ingest_date DATE,
            id STRING,
            data VARIANT
        )
        """
    )


def write_jsonl_and_snowflake(
    path: Path,
    rows: Iterable[Dict[str, Any]],
    *,
    table_name: str,
    indicator: str,
    ingest_date: str,
    batch_size: int,
) -> int:
    config = get_snowflake_config()
    conn = connect_to_snowflake(config)
    try:
        cursor = conn.cursor()
        ensure_snowflake_table(cursor, table_name)

        n = 0
        batch = []
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                payload = json.dumps(row, ensure_ascii=False)
                f.write(payload)
                f.write("\n")
                batch.append((indicator, ingest_date, row.get("Id"), payload))
                n += 1

                if len(batch) >= batch_size:
                    cursor.executemany(
                        f"INSERT INTO {table_name} (indicator, ingest_date, id, data) "
                        "SELECT %s, %s, %s, parse_json(%s)",
                        batch,
                    )
                    batch.clear()

        if batch:
            cursor.executemany(
                f"INSERT INTO {table_name} (indicator, ingest_date, id, data) "
                "SELECT %s, %s, %s, parse_json(%s)",
                batch,
            )

        conn.commit()
        return n
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest WHO GHO API indicator data to raw JSONL.")
    parser.add_argument("--indicator", required=True, help="Indicator code / endpoint (e.g., MDG_0000000007)")
    parser.add_argument("--page-size", type=int, default=1000, help="Rows per page ($top). Default 1000.")
    parser.add_argument("--top", type=int, default=None, help="Max total rows to fetch (for testing).")
    parser.add_argument(
        "--select",
        type=str,
        default=None,
        help="OData $select comma-separated (optional). Example: Id,SpatialDim,TimeDim,NumericValue",
