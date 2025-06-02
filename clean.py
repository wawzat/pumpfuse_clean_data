"""
Pumpfuse Google Sheets Cleaner

This script connects to a Google Sheet containing pumpfuse logger data, detects missing timestamps based on delta analysis, and interpolates missing rows as needed. Cleaned rows are marked in column D.

Usage:
    python clean.py <start_row_number>

Configuration:
    - Requires a config.ini file with Google API credentials and sheet name.
    - See README.md for setup instructions.

PEP 257 compliant.
"""

import sys
import configparser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
from tqdm import tqdm

CLEANED_MARK = 'cleaned'
DELTA_AVG_WINDOW = 5
DELTA_TOLERANCE = 0.2  # 20% tolerance for delta comparison


def read_config():
    """Read configuration from config.ini."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['google']


def get_gsheet(sheet_name, credentials_json):
    """Authenticate and return the worksheet object."""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    return sheet


def parse_timestamp(ts_str):
    """Parse timestamp string to datetime object."""
    return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')


def format_timestamp(dt):
    """Format datetime object to string for Google Sheets, with single digit hours unpadded, and force Sheets datetime format."""
    # Format as 'YYYY-MM-DD H:MM:SS' (no leading zero for hour)
    # Prefix with single quote to force Google Sheets to treat as datetime
    # But single quote makes it text, so instead, return as formula: =TEXT(...)
    # However, best is to return as: =DATE(YYYY,MM,DD)+TIME(H,MM,SS)
    return f"=DATE({dt.year},{dt.month},{dt.day})+TIME({dt.hour},{dt.minute},{dt.second})"


def get_float(val):
    """Convert value to float if possible, else None."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def clean_sheet(sheet, start_row, total_writes=None):
    """Main cleaning logic for the Google Sheet with rate limiting."""
    data = sheet.get_all_values()
    row = start_row
    total_rows = len(data)
    rows_added = 0
    write_ops = 0
    # If total_writes is provided, use it for the progress bar; else fall back to row count
    pbar_total = total_writes if total_writes is not None else (total_rows - start_row)
    with tqdm(total=pbar_total, desc="Processing writes", unit=" writes") as pbar:
        while row < len(data):
            prev_deltas = []
            for i in range(row - DELTA_AVG_WINDOW, row):
                if i > 1:
                    delta = get_float(data[i][2])
                    if delta is not None:
                        prev_deltas.append(delta)
            if len(prev_deltas) < DELTA_AVG_WINDOW:
                row += 1
                continue
            avg_delta = sum(prev_deltas) / DELTA_AVG_WINDOW
            curr_delta = get_float(data[row][2])
            if curr_delta is None:
                row += 1
                continue
            n_missing = round(curr_delta / avg_delta)
            if n_missing > 1 and abs(curr_delta - n_missing * avg_delta) < DELTA_TOLERANCE * avg_delta * n_missing:
                prev_ts = parse_timestamp(data[row - 1][1])
                for n in range(1, n_missing):
                    new_ts = prev_ts + timedelta(hours=avg_delta * n)
                    insert_row = [
                        '',
                        format_timestamp(new_ts),
                        f'=IF(ISDATE(B{row+1}),ROUND((B{row+1}-B{row})*24,2),)',
                        CLEANED_MARK
                    ]
                    sheet.insert_row(insert_row, row + n, value_input_option='USER_ENTERED')
                    time.sleep(1.2)
                    rows_added += 1
                    write_ops += 1
                    pbar.update(1)
                updated_delta_formula = f'=IF(ISDATE(B{row + n_missing}),ROUND((B{row + n_missing}-B{row + n_missing - 1})*24,2),)'
                sheet.update_cell(row + n_missing, 3, updated_delta_formula)
                time.sleep(1.2)
                write_ops += 1
                pbar.update(1)
                data = sheet.get_all_values()
                row += n_missing
            else:
                row += 1
    return rows_added


def estimate_rows_to_insert(data, start_row):
    """Preprocess the data to estimate how many rows will be inserted during cleaning."""
    row = start_row
    rows_to_insert = 0
    update_ops = 0
    while row < len(data):
        prev_deltas = []
        for i in range(row - DELTA_AVG_WINDOW, row):
            if i > 1:
                delta = get_float(data[i][2])
                if delta is not None:
                    prev_deltas.append(delta)
        if len(prev_deltas) < DELTA_AVG_WINDOW:
            row += 1
            continue
        avg_delta = sum(prev_deltas) / DELTA_AVG_WINDOW
        curr_delta = get_float(data[row][2])
        if curr_delta is None:
            row += 1
            continue
        n_missing = round(curr_delta / avg_delta)
        if n_missing > 1 and abs(curr_delta - n_missing * avg_delta) < DELTA_TOLERANCE * avg_delta * n_missing:
            rows_to_insert += n_missing - 1
            update_ops += 1  # for the delta formula update
            row += n_missing
        else:
            row += 1
    return rows_to_insert, update_ops


def estimate_processing_time(sheet, start_row):
    """Estimate the number of write operations and total time required, using preprocessing for accuracy."""
    data = sheet.get_all_values()
    rows_to_insert, update_ops = estimate_rows_to_insert(data, start_row)
    # Each insert and update takes at least 1.2s
    total_writes = rows_to_insert + update_ops
    estimated_seconds = total_writes * 1.2
    return rows_to_insert, update_ops, estimated_seconds


def main():
    """Entry point for the script."""
    if len(sys.argv) != 2:
        print('Usage: python clean.py <start_row_number>')
        sys.exit(1)
    start_row = int(sys.argv[1])
    config = read_config()
    sheet = get_gsheet(config['sheet_name'], config['credentials_json'])
    rows_to_insert, update_ops, estimated_seconds = estimate_processing_time(sheet, start_row)
    estimated_minutes = estimated_seconds / 60
    total_writes = rows_to_insert + update_ops
    print(f"Estimated rows to insert: {rows_to_insert}")
    print(f"Estimated delta formula updates: {update_ops}")
    print(f"Estimated time: {estimated_minutes:.1f} minutes ({estimated_seconds:.0f} seconds)")
    if estimated_seconds > 300:
        proceed = input("Warning: Estimated time exceeds 5 minutes. Continue? (y/n): ").strip().lower()
        if proceed != 'y':
            print('Aborted by user.')
            sys.exit(0)
    rows_added = clean_sheet(sheet, start_row, total_writes=total_writes)
    print(f'Cleaning complete. Rows added: {rows_added}')


if __name__ == '__main__':
    main()
