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
