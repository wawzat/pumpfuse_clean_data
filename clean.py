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


def clean_sheet(sheet, start_row):
    """Main cleaning logic for the Google Sheet with rate limiting."""
    data = sheet.get_all_values()
    row = start_row
    total_rows = len(data)
    with tqdm(total=total_rows - start_row, desc="Processing rows", unit="row") as pbar:
        while row < len(data):
            # Get previous DELTA_AVG_WINDOW delta values
            prev_deltas = []
            for i in range(row - DELTA_AVG_WINDOW, row):
                if i > 1:  # skip header and empty rows
                    delta = get_float(data[i][2])
                    if delta is not None:
                        prev_deltas.append(delta)
            if len(prev_deltas) < DELTA_AVG_WINDOW:
                row += 1
                pbar.update(1)
                continue
            avg_delta = sum(prev_deltas) / DELTA_AVG_WINDOW
            curr_delta = get_float(data[row][2])
            if curr_delta is None:
                row += 1
                pbar.update(1)
                continue
            # Check if current delta is approximately n * avg_delta
            n_missing = round(curr_delta / avg_delta)
            if n_missing > 1 and abs(curr_delta - n_missing * avg_delta) < DELTA_TOLERANCE * avg_delta * n_missing:
                # Insert n_missing - 1 rows above current row, mark only inserted rows as cleaned
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
                    time.sleep(1.2)  # Rate limit: 1 write per 1.2 seconds
                # Do NOT mark the current row as cleaned
                # Refresh data after insertion
                data = sheet.get_all_values()
                row += n_missing
                pbar.update(n_missing)
            else:
                row += 1
                pbar.update(1)


def estimate_processing_time(sheet, start_row):
    """Estimate the number of rows to process and total time required."""
    data = sheet.get_all_values()
    total_rows = len(data) - start_row
    # Each row may require at least one write (1.2s), but in worst case (insertion) more
    # For estimation, assume 1.2s per row as a lower bound
    estimated_seconds = total_rows * 1.2
    return total_rows, estimated_seconds


def main():
    """Entry point for the script."""
    if len(sys.argv) != 2:
        print('Usage: python clean.py <start_row_number>')
        sys.exit(1)
    start_row = int(sys.argv[1])
    config = read_config()
    sheet = get_gsheet(config['sheet_name'], config['credentials_json'])
    total_rows, estimated_seconds = estimate_processing_time(sheet, start_row)
    estimated_minutes = estimated_seconds / 60
    print(f"Rows to process: {total_rows}")
    print(f"Estimated time: {estimated_minutes:.1f} minutes ({estimated_seconds:.0f} seconds)")
    if estimated_seconds > 300:
        proceed = input("Warning: Estimated time exceeds 5 minutes. Continue? (y/n): ").strip().lower()
        if proceed != 'y':
            print('Aborted by user.')
            sys.exit(0)
    clean_sheet(sheet, start_row)
    print('Cleaning complete.')


if __name__ == '__main__':
    main()
