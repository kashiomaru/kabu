#!/usr/bin/env python3
"""
Read codes (first column) from a CSV, fetch market cap for each, and write
the values back into the CSV as a new column next to price. No console output.

Usage:
  python3 extract_codes.py               # uses ./2025_09_01.csv by default
  python3 extract_codes.py <path/to.csv> # specify an input CSV path
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import List, Optional, Dict

import requests
from bs4 import BeautifulSoup


def extract_codes_from_csv(csv_path: Path) -> List[str]:
    """Read the CSV file and return a list of codes from the first column.

    The function skips the header row if the first cell equals "コード".
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    codes: List[str] = []

    # Use utf-8-sig to gracefully handle a potential BOM
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row_index, row in enumerate(reader):
            if not row:
                continue
            first_cell = str(row[0]).strip()
            if row_index == 0 and first_cell == "コード":
                continue
            if first_cell:
                codes.append(first_cell)

    return codes


@dataclass
class StockDataResult:
    code: str
    market_cap_text: Optional[str]
    industry: Optional[str]
    summary: Optional[str]
    error: Optional[str] = None


def fetch_stock_data_for_code(code: str, session: requests.Session) -> StockDataResult:
    """Fetch the kabutan page for a given code and parse market cap, industry, and summary.

    Returns StockDataResult with market_cap_text like "243億円", industry, and summary or None on failure.
    """
    url = f"https://kabutan.jp/stock/?code={code}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
    }
    try:
        resp = session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return StockDataResult(code=code, market_cap_text=None, industry=None, summary=None, error=f"HTTP error: {exc}")

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Parse market cap
        th_nodes = soup.find_all("th", class_="v_zika1")
        market_cap_text: Optional[str] = None
        for th in th_nodes:
            if th.get_text(strip=True) == "時価総額":
                td = th.find_next_sibling("td", class_="v_zika2")
                if td:
                    text = td.get_text(strip=True)
                    market_cap_text = re.sub(r"\s+", "", text)
                break
        
        # Parse industry
        industry: Optional[str] = None
        industry_link = soup.find("a", href=re.compile(r"/themes/\?industry=\d+"))
        if industry_link:
            industry = industry_link.get_text(strip=True)
        
        # Parse summary
        summary: Optional[str] = None
        summary_th = soup.find("th", string="概要")
        if summary_th:
            summary_td = summary_th.find_next_sibling("td")
            if summary_td:
                summary = summary_td.get_text(strip=True)
        
        return StockDataResult(
            code=code, 
            market_cap_text=market_cap_text, 
            industry=industry, 
            summary=summary
        )
    except Exception as exc:  # noqa: BLE001
        return StockDataResult(
            code=code, 
            market_cap_text=None, 
            industry=None, 
            summary=None, 
            error=f"Parse error: {exc}"
        )


def normalize_market_cap_to_oku_number(text: Optional[str]) -> Optional[str]:
    """Normalize market cap like "1兆2,882億円" to an oku-based number string.

    Examples:
      "1兆2,882億円" -> "12882"
      "705億円" -> "705"
      "78.3億円" -> "78.3"
    Returns None if input cannot be parsed.
    """
    if not text:
        return None
    s = text.strip().replace(",", "")
    # Extract optional "兆" and optional "億"
    m = re.search(r"(?:(?P<cho>[0-9]+(?:\.[0-9]+)?)兆)?(?:(?P<oku>[0-9]+(?:\.[0-9]+)?)億)?円?", s)
    if not m:
        return None
    cho_part = m.group("cho")
    oku_part = m.group("oku")
    try:
        total_oku = 0.0
        if cho_part is not None:
            total_oku += float(cho_part) * 10000.0
        if oku_part is not None:
            total_oku += float(oku_part)
        # Format without scientific notation, drop trailing zeros
        formatted = ("%f" % total_oku).rstrip("0").rstrip(".")
        return formatted
    except Exception:
        return None


def csv_has_required_columns(csv_path: Path, required_columns: List[str] = None) -> bool:
    """Return True if the CSV header already contains all required columns."""
    if required_columns is None:
        required_columns = ["時価総額", "業種", "概要"]
    
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        return all(col in header for col in required_columns)


def update_csv_with_stock_data(
    csv_path: Path,
    code_to_data: Dict[str, Dict[str, Optional[str]]],
    insert_after_column: str = "利回り",
) -> None:
    """Insert new columns with stock data after a target column and save in place.

    If the CSV already includes all required columns, this function will do nothing.
    """
    # Read all rows
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return

    header = rows[0]
    
    # Check if all required columns already exist
    required_columns = ["時価総額", "業種", "概要"]
    if all(col in header for col in required_columns):
        return

    # Find insert position (after 利回り)
    insert_idx = len(header)
    if insert_after_column in header:
        insert_idx = header.index(insert_after_column) + 1

    # Build new header - add missing columns
    new_header = header[:insert_idx]
    for col in required_columns:
        if col not in header:
            new_header.append(col)
    new_header.extend(header[insert_idx:])

    # Build new data rows
    new_rows = [new_header]
    for row in rows[1:]:
        if not row:
            new_rows.append(row)
            continue
        
        code_value = str(row[0]).strip() if len(row) > 0 else ""
        stock_data = code_to_data.get(code_value, {})
        
        # Build new row with existing data
        new_row = row[:insert_idx]
        
        # Add missing columns
        for col in required_columns:
            if col not in header:
                value = stock_data.get(col, "")
                new_row.append(value)
        
        new_row.extend(row[insert_idx:])
        new_rows.append(new_row)

    # Write back
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(new_rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch market cap, industry, and summary for codes in CSV and update the CSV in place.")
    parser.add_argument(
        "csv_path",
        nargs="?",
        help="Path to input CSV. Defaults to scripts/2025_09_01.csv",
    )
    args = parser.parse_args(argv)

    default_csv = Path(__file__).resolve().parent / "2025_09_01.csv"
    input_path = Path(args.csv_path).expanduser().resolve() if args.csv_path else default_csv

    # If CSV already includes all required columns, skip any fetching and exit gracefully
    try:
        if csv_has_required_columns(input_path):
            return 0
    except Exception as exc:  # noqa: BLE001
        # Keep errors on stderr
        print(f"Failed to read CSV header: {exc}", file=sys.stderr)
        return 1

    try:
        codes = extract_codes_from_csv(input_path)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 Keep robust for CLI entrypoint
        print(f"Failed to read CSV: {exc}", file=sys.stderr)
        return 1

    session = requests.Session()
    code_to_data: Dict[str, Dict[str, Optional[str]]] = {}

    for idx, code in enumerate(codes):
        result = fetch_stock_data_for_code(code, session)
        normalized_market_cap = normalize_market_cap_to_oku_number(result.market_cap_text)
        
        code_to_data[code] = {
            "時価総額": normalized_market_cap,
            "業種": result.industry,
            "概要": result.summary
        }

        # Random interval 0.3 - 0.5 seconds after each request
        if idx != len(codes) - 1:
            time.sleep(random.uniform(0.3, 0.5))

    # Update CSV in place, inserting the new columns after 利回り
    try:
        update_csv_with_stock_data(input_path, code_to_data)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to update CSV with stock data: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


