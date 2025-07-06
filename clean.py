"""
Pumpfuse Google Sheets Cleaner

This script connects to a Google Sheet containing pumpfuse logger data, detects missing timestamps based on delta analysis, and interpolates missing rows as needed. Cleaned rows are marked in column D.

Usage:
    python clean.py <start_row_number>

Configuration:
    - Requires a config.ini file with Google API credentials and sheet name.
    - See README.md for setup instructions.

"""

import sys
import configparser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import logging
from typing import Any, Dict, Tuple

CLEANED_MARK = 'cleaned'
DELTA_AVG_WINDOW = 5
DELTA_TOLERANCE = 0.2  # 20% tolerance for delta comparison

# Set up logging to a file
logging.basicConfig(filename='clean_errors.log', level=logging.ERROR, 
                    format='%(asctime)s %(levelname)s: %(message)s')

def read_config() -> Dict[str, Any]:
    """Read Google Sheets configuration from config.ini."""
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        return config['google']
    except KeyError as e:
        logging.error(f"Missing section or key in config: {e}")
        print('Configuration file is missing required section or key. See clean_errors.log for details.')
        sys.exit(1)
    except configparser.Error as e:
        logging.error(f"ConfigParser error: {e}")
        print('Failed to parse configuration file. See clean_errors.log for details.')
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading config: {e}")
        print('Failed to read configuration. See clean_errors.log for details.')
        sys.exit(1)

def get_gsheet(target_sheet_name: str, credentials_json: str) -> gspread.Worksheet:
    """Connect to Google Sheets and return the worksheet object."""
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope)
        client = gspread.authorize(creds)
        sheet = client.open(target_sheet_name).sheet1
        return sheet
    except Exception as e:
        logging.error(f"Error connecting to Google Sheet: {e}")
        print('Failed to connect to Google Sheet. See clean_errors.log for details.')
        sys.exit(1)

def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp string to datetime object."""
    return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')

def format_timestamp(dt: datetime) -> str:
    """Format datetime object to string for Google Sheets, with single digit hours unpadded, and force Sheets datetime format."""
    return f"=DATE({dt.year},{dt.month},{dt.day})+TIME({dt.hour},{dt.minute},{dt.second})"

def get_float(val: Any) -> float | None:
    """Convert value to float if possible, else None."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def clean_sheet(sheet: gspread.Worksheet, start_row: int, total_writes: int | None = None) -> int:
    """Clean the Google Sheet by interpolating missing rows and updating delta formulas."""
    try:
        data = sheet.get_all_values()
        row = start_row
        total_rows = len(data)
        rows_added = 0
        write_ops = 0
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
                        insert_row_index = row + n
                        # Insert CLEANED_MARK in column G (index 7)
                        insert_row = [
                            '',
                            format_timestamp(new_ts),
                            f'=IF(ISDATE(B{insert_row_index}),ROUND((B{insert_row_index}-B{insert_row_index - 1})*24,2),)',
                            '', '', '', CLEANED_MARK
                        ]
                        try:
                            sheet.insert_row(insert_row, insert_row_index, value_input_option='USER_ENTERED')
                        except Exception as e:
                            logging.error(f"Error inserting row at {insert_row_index}: {e}")
                            print(f"Error inserting row at {insert_row_index}. See clean_errors.log for details.")
                            continue
                        time.sleep(1.2)
                        rows_added += 1
                        write_ops += 1
                        pbar.update(1)
                    updated_delta_formula = f'=IF(ISDATE(B{row + n_missing}),ROUND((B{row + n_missing}-B{row + n_missing - 1})*24,2),)'
                    try:
                        sheet.update_cell(row + n_missing, 3, updated_delta_formula)
                    except Exception as e:
                        logging.error(f"Error updating delta formula at row {row + n_missing}: {e}")
                        print(f"Error updating delta formula at row {row + n_missing}. See clean_errors.log for details.")
                    time.sleep(1.2)
                    write_ops += 1
                    pbar.update(1)
                    data = sheet.get_all_values()
                    row += n_missing
                else:
                    row += 1
        return rows_added
    except Exception as e:
        logging.error(f"Error in clean_sheet: {e}")
        print('An error occurred during cleaning. See clean_errors.log for details.')
        return 0

def estimate_rows_to_insert(data: list[list[Any]], start_row: int) -> Tuple[int, int]:
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

def estimate_processing_time(sheet: gspread.Worksheet, start_row: int) -> Tuple[int, int, float]:
    """Estimate the number of write operations and total time required, using preprocessing for accuracy."""
    data = sheet.get_all_values()
    rows_to_insert, update_ops = estimate_rows_to_insert(data, start_row)
    total_writes = rows_to_insert + update_ops
    estimated_seconds = total_writes * 1.2
    return rows_to_insert, update_ops, estimated_seconds

def main() -> None:
    """Main entry point for the cleaning script."""
    try:
        import signal
        def handle_sigint(sig, frame):
            print('\nInterrupted by user. Exiting gracefully...')
            sys.exit(0)
        signal.signal(signal.SIGINT, handle_sigint)

        if len(sys.argv) != 2:
            print('Usage: python clean.py <start_row_number>')
            sys.exit(1)
        try:
            start_row = int(sys.argv[1])
        except ValueError:
            print('Start row number must be an integer.')
            sys.exit(1)
        config = read_config()
        sheet = get_gsheet(config['target_sheet_name'], config['credentials_json'])
        rows_to_process = len(sheet.get_all_values()) - start_row
        print(f"Rows to process: {rows_to_process}")
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
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
        print('A fatal error occurred. See clean_errors.log for details.')

if __name__ == '__main__':
    main()
