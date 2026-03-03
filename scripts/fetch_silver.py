#!/usr/bin/env python3
"""Download and parse COMEX silver inventory XLS, update data/inventory.json."""

import json
import os
import sys
from datetime import date
from pathlib import Path

import requests
import xlrd

URL = "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls"
REPO_ROOT = Path(__file__).resolve().parent.parent
ARCHIVES_DIR = REPO_ROOT / "data" / "archives"
INVENTORY_FILE = REPO_ROOT / "data" / "inventory.json"


def download_xls(dest_path: Path) -> None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(URL, headers=headers, timeout=30)
    response.raise_for_status()
    dest_path.write_bytes(response.content)
    print(f"Downloaded {len(response.content):,} bytes → {dest_path}")


def parse_xls(xls_path: Path) -> dict:
    wb = xlrd.open_workbook(str(xls_path))
    ws = wb.sheet_by_index(0)

    # Extract activity date from the header area (first ~10 rows)
    activity_date = None
    for row_idx in range(min(10, ws.nrows)):
        for col_idx in range(ws.ncols):
            cell = ws.cell_value(row_idx, col_idx)
            if isinstance(cell, str) and "activity date" in cell.lower():
                # The date is typically in the next cell or same row next col
                for c in range(col_idx + 1, ws.ncols):
                    val = ws.cell_value(row_idx, c)
                    if val and val != "":
                        if isinstance(val, float):
                            # xlrd returns dates as floats
                            dt = xlrd.xldate_as_datetime(val, wb.datemode)
                            activity_date = dt.strftime("%Y-%m-%d")
                        elif isinstance(val, str) and val.strip():
                            activity_date = val.strip()
                        break
                if activity_date:
                    break
        if activity_date:
            break

    if not activity_date:
        # Fall back to today
        activity_date = date.today().isoformat()
        print(f"Warning: could not find activity date in XLS, using {activity_date}")

    # Labels to search for (case-insensitive, strip whitespace)
    targets = {
        "total registered": "registered",
        "total eligible": "eligible",
        "combined total": "combined",
    }

    result = {"activity_date": activity_date}

    for row_idx in range(ws.nrows):
        for col_idx in range(ws.ncols):
            cell_val = ws.cell_value(row_idx, col_idx)
            if not isinstance(cell_val, str):
                continue
            label = cell_val.strip().lower()
            for target, key in targets.items():
                if target in label:
                    # Collect numeric values from remaining columns in this row
                    nums = []
                    for c in range(col_idx + 1, ws.ncols):
                        v = ws.cell_value(row_idx, c)
                        if isinstance(v, (int, float)) and v != "":
                            nums.append(int(v))
                    if len(nums) >= 2:
                        result[f"prev_{key}"] = nums[0]
                        result[key] = nums[1]
                    elif len(nums) == 1:
                        result[key] = nums[0]
                    break

    # Validate all required fields
    required = ["registered", "eligible", "combined"]
    missing = [k for k in required if k not in result]
    if missing:
        raise ValueError(f"Failed to parse fields: {missing}")

    return result


def update_inventory(record: dict) -> bool:
    """Append record to inventory.json. Returns False if date already exists."""
    if INVENTORY_FILE.exists():
        data = json.loads(INVENTORY_FILE.read_text())
    else:
        data = []

    # Dedup by activity_date
    existing_dates = {entry["activity_date"] for entry in data}
    if record["activity_date"] in existing_dates:
        print(f"Date {record['activity_date']} already in inventory — skipping.")
        return False

    data.append(record)
    # Keep sorted by date ascending
    data.sort(key=lambda x: x["activity_date"])
    INVENTORY_FILE.write_text(json.dumps(data, indent=2))
    print(f"Updated inventory.json ({len(data)} records total)")
    return True


def main():
    ARCHIVES_DIR.mkdir(parents=True, exist_ok=True)

    # Use today as filename; if we later parse a different activity_date we rename
    today_str = date.today().isoformat()
    tmp_path = ARCHIVES_DIR / f"{today_str}_Silver_stocks.xls"

    print(f"Downloading {URL} ...")
    download_xls(tmp_path)

    print("Parsing XLS ...")
    record = parse_xls(tmp_path)
    print(f"Parsed record: {record}")

    # Rename archive file to match the activity date from the file
    activity_date = record["activity_date"]
    final_path = ARCHIVES_DIR / f"{activity_date}_Silver_stocks.xls"
    if tmp_path != final_path:
        tmp_path.rename(final_path)
        print(f"Renamed archive → {final_path.name}")

    # Update JSON
    added = update_inventory(record)
    if not added:
        sys.exit(0)

    print("Done.")


if __name__ == "__main__":
    main()
