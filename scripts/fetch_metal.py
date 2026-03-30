#!/usr/bin/env python3
"""Download and parse COMEX metal inventory XLS, update data/<metal>_inventory.json."""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests
import xlrd

METALS = {
    "silver": {
        "url": "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls",
        "filename": "Silver_stocks.xls",
        "integer_values": True,
    },
    "gold": {
        "url": "https://www.cmegroup.com/delivery_reports/Gold_Stocks.xls",
        "filename": "Gold_Stocks.xls",
        "integer_values": False,
    },
}

REPO_ROOT = Path(__file__).resolve().parent.parent

MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds between retries


def download_xls(url: str, dest_path: Path) -> None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.cmegroup.com/",
        "Connection": "keep-alive",
    }

    session = requests.Session()
    session.headers.update(headers)

    # Prime the session with a visit to the CME homepage to get cookies
    try:
        session.get("https://www.cmegroup.com/", timeout=15)
    except requests.exceptions.RequestException:
        pass  # Best-effort; proceed anyway

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            dest_path.write_bytes(response.content)
            print(f"Downloaded {len(response.content):,} bytes → {dest_path}")
            return
        except requests.exceptions.RequestException as e:
            last_exc = e
            print(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)

    # Clean up any partial file before raising
    if dest_path.exists():
        dest_path.unlink()
        print(f"Removed partial file: {dest_path}")

    raise RuntimeError(f"Failed to download {url} after {MAX_RETRIES} attempts: {last_exc}") from last_exc


def parse_xls(xls_path: Path, integer_values: bool) -> dict:
    try:
        wb = xlrd.open_workbook(str(xls_path))
    except xlrd.XLRDError as e:
        raise RuntimeError(
            f"Could not open {xls_path.name} as a valid XLS file. "
            f"The server may have returned an error page. Details: {e}"
        ) from e

    ws = wb.sheet_by_index(0)

    # Find activity date
    activity_date = None
    for row_idx in range(min(15, ws.nrows)):
        for col_idx in range(ws.ncols):
            cell = ws.cell_value(row_idx, col_idx)
            if isinstance(cell, str) and "activity date" in cell.lower():
                import re
                m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", cell)
                if m:
                    from datetime import datetime
                    dt = datetime.strptime(m.group(1), "%m/%d/%Y")
                    activity_date = dt.strftime("%Y-%m-%d")
                    break
                for c in range(col_idx + 1, ws.ncols):
                    val = ws.cell_value(row_idx, c)
                    if isinstance(val, float) and val > 0:
                        dt = xlrd.xldate_as_datetime(val, wb.datemode)
                        activity_date = dt.strftime("%Y-%m-%d")
                        break
                    elif isinstance(val, str) and val.strip():
                        activity_date = val.strip()
                        break
            if activity_date:
                break
        if activity_date:
            break

    if not activity_date:
        activity_date = date.today().isoformat()
        print(f"Warning: could not find activity date, using {activity_date}")

    targets = {
        "total registered": "registered",
        "total pledged":    "pledged",
        "total eligible":   "eligible",
        "combined total":   "combined",
    }

    def coerce(v):
        if not isinstance(v, (int, float)) or v == "":
            return None
        return int(round(v)) if integer_values else round(float(v), 3)

    result = {"activity_date": activity_date}

    for row_idx in range(ws.nrows):
        for col_idx in range(ws.ncols):
            cell_val = ws.cell_value(row_idx, col_idx)
            if not isinstance(cell_val, str):
                continue
            label = cell_val.strip().lower()
            for target, key in targets.items():
                if label.startswith(target):
                    nums = []
                    for c in range(col_idx + 1, ws.ncols):
                        v = ws.cell_value(row_idx, c)
                        cv = coerce(v)
                        if cv is not None:
                            nums.append(cv)
                    if len(nums) >= 2:
                        result[f"prev_{key}"] = nums[0]
                        result[key] = nums[-1]
                    elif len(nums) == 1:
                        result[key] = nums[0]
                    break

    required = ["registered", "eligible", "combined"]
    missing = [k for k in required if k not in result]
    if missing:
        raise ValueError(
            f"Failed to parse required fields: {missing}. "
            f"The spreadsheet format may have changed. "
            f"Fields found: {list(result.keys())}"
        )

    return result


def update_inventory(inventory_file: Path, record: dict) -> bool:
    if inventory_file.exists():
        data = json.loads(inventory_file.read_text())
    else:
        data = []

    existing_dates = {entry["activity_date"] for entry in data}
    if record["activity_date"] in existing_dates:
        print(f"Date {record['activity_date']} already in inventory — skipping.")
        return False

    data.append(record)
    data.sort(key=lambda x: x["activity_date"])
    inventory_file.write_text(json.dumps(data, indent=2))
    print(f"Updated {inventory_file.name} ({len(data)} records total)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Fetch COMEX metal inventory")
    parser.add_argument("--metal", choices=list(METALS.keys()), required=True)
    args = parser.parse_args()

    cfg = METALS[args.metal]
    archives_dir = REPO_ROOT / "data" / f"{args.metal}_archives"
    inventory_file = REPO_ROOT / "data" / f"{args.metal}_inventory.json"

    archives_dir.mkdir(parents=True, exist_ok=True)

    today_str = date.today().isoformat()
    tmp_path = archives_dir / f"{today_str}_{cfg['filename']}"

    print(f"[{args.metal.upper()}] Downloading {cfg['url']} ...")
    try:
        download_xls(cfg["url"], tmp_path)
    except RuntimeError as e:
        print(f"ERROR: Download failed — {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[{args.metal.upper()}] Parsing XLS ...")
    try:
        record = parse_xls(tmp_path, cfg["integer_values"])
    except (RuntimeError, ValueError) as e:
        print(f"ERROR: Parse failed — {e}", file=sys.stderr)
        # Keep the archive file so it can be inspected manually
        print(f"Archive kept for inspection: {tmp_path}", file=sys.stderr)
        sys.exit(2)

    print(f"Parsed: {record}")

    activity_date = record["activity_date"]
    final_path = archives_dir / f"{activity_date}_{cfg['filename']}"
    if tmp_path != final_path:
        tmp_path.rename(final_path)
        print(f"Renamed archive → {final_path.name}")

    update_inventory(inventory_file, record)
    print("Done.")


if __name__ == "__main__":
    main()
