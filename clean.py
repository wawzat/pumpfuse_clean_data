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
    """Format datetime object to string for Google Sheets."""
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def get_float(val):
    """Convert value to float if possible, else None."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def clean_sheet(sheet, start_row):
    """Main cleaning logic for the Google Sheet."""
    data = sheet.get_all_values()
    row = start_row
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
            continue
        avg_delta = sum(prev_deltas) / DELTA_AVG_WINDOW
        curr_delta = get_float(data[row][2])
        if curr_delta is None:
            row += 1
            continue
        # Check if current delta is approximately n * avg_delta
        n_missing = round(curr_delta / avg_delta)
        if n_missing > 1 and abs(curr_delta - n_missing * avg_delta) < DELTA_TOLERANCE * avg_delta * n_missing:
            # Insert n_missing - 1 rows above current row
            prev_ts = parse_timestamp(data[row - 1][1])
            for n in range(1, n_missing):
                new_ts = prev_ts + timedelta(hours=avg_delta * n)
                insert_row = [
                    '',
                    format_timestamp(new_ts),
                    f'=IF(ISDATE(B{row+1}),ROUND((B{row+1}-B{row})*24,2),)',
                    CLEANED_MARK
                ]
                sheet.insert_row(insert_row, row + n)
            # Mark the current row as cleaned
            sheet.update_cell(row + n_missing, 4, CLEANED_MARK)
            # Refresh data after insertion
            data = sheet.get_all_values()
            row += n_missing
        else:
            # Mark as cleaned if not already
            if data[row][3].strip().lower() != CLEANED_MARK:
                sheet.update_cell(row + 1, 4, CLEANED_MARK)
            row += 1


def main():
    """Entry point for the script."""
    if len(sys.argv) != 2:
        print('Usage: python clean.py <start_row_number>')
        sys.exit(1)
    start_row = int(sys.argv[1])
    config = read_config()
    sheet = get_gsheet(config['sheet_name'], config['credentials_json'])
    clean_sheet(sheet, start_row)
    print('Cleaning complete.')


if __name__ == '__main__':
    main()
