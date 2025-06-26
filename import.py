"""
import.py

This script modifies a Google Sheet by converting 'Time' values from US Eastern to US Pacific timezone, rounding to the nearest minute, and formatting as 'YYYY-mm-dd h:m:00'.
It then deletes all rows in the input sheet up to and including the most recent datetime found in the 'Timestamp' column of the target sheet.

Usage:
    python import.py

Requirements:
    - config.ini with appropriate Google Sheets and credentials configuration
    - See requirements.txt for dependencies
"""

import argparse
import configparser
import logging
from datetime import datetime, timedelta
from typing import Any, List, Optional
import pytz
import gspread
from gspread.worksheet import Worksheet
from oauth2client.service_account import ServiceAccountCredentials
import re

# Set up logging
logging.basicConfig(
    filename='clean_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)


def load_config(config_path: str = 'config.ini') -> configparser.ConfigParser:
    """Load configuration from the given .ini file."""
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def get_gspread_client(credentials_path: str) -> gspread.Client:
    """Authenticate and return a gspread client."""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    return gspread.authorize(creds)


def convert_time_eastern_to_pacific(time_str: str) -> Optional[str]:
    """Convert a time string from US Eastern to US Pacific, rounding to nearest minute, formatted as 'YYYY-mm-dd h:m:00'."""
    try:
        eastern = pytz.timezone('US/Eastern')
        pacific = pytz.timezone('US/Pacific')
        # Try parsing with various formats, including 'Jun 22, 2025, 1:45:08 PM'
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%b %d, %Y, %I:%M:%S %p',
            '%b %d, %Y, %I:%M %p'
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(time_str, fmt)
                break
            except ValueError:
                continue
        else:
            logging.error(f"Unrecognized time format: {time_str}")
            return None
        dt_eastern = eastern.localize(dt)
        dt_pacific = dt_eastern.astimezone(pacific)
        # Round to nearest minute
        if dt_pacific.second >= 30:
            dt_pacific = dt_pacific.replace(second=0) + timedelta(minutes=1)
        else:
            dt_pacific = dt_pacific.replace(second=0)
        # Format: YYYY-mm-dd h:m:00 (no leading zero for hour, cross-platform)
        formatted = dt_pacific.strftime('%Y-%m-%d %H:%M:00')
        # Remove leading zero from hour if present
        formatted = formatted.replace(' 0', ' ')
        return formatted
    except Exception as e:
        logging.error(f"Error converting time '{time_str}': {e}")
        return None


def get_most_recent_timestamp(ws: Worksheet, timestamp_col: str = 'Timestamp', expected_headers: Optional[List[str]] = None) -> Optional[datetime]:
    """
    Get the most recent datetime from the given worksheet's timestamp column.
    Handles duplicate headers gracefully. Allows a single empty header if present, as in the case of a blank column A.
    Optionally accepts expected_headers to override worksheet headers.
    """
    try:
        if expected_headers:
            records = ws.get_all_records(expected_headers=expected_headers)
            headers = expected_headers
        else:
            records = ws.get_all_records()
            headers = list(records[0].keys()) if records else []
        # Check for duplicate headers (allow a single empty string)
        header_counts = {}
        for h in headers:
            header_counts[h] = header_counts.get(h, 0) + 1
        duplicates = [k for k, v in header_counts.items() if v > 1]
        if duplicates:
            logging.error(f"Header row contains duplicate values: {headers}")
            return None
        timestamps = [r[timestamp_col] for r in records if r.get(timestamp_col)]
        # Try parsing all timestamps
        dt_list = []
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%b %d, %Y, %I:%M:%S %p',
            '%b %d, %Y, %I:%M %p'
        ]
        for t in timestamps:
            for fmt in formats:
                try:
                    dt = datetime.strptime(t, fmt)
                    dt_list.append(dt)
                    break
                except ValueError:
                    continue
        if not dt_list:
            return None
        return max(dt_list)
    except Exception as e:
        logging.error(f"Error finding most recent timestamp: {e}")
        return None


def update_time_column(input_ws: Worksheet, time_col: str, input_records: List[dict]) -> None:
    """
    Batch update the Time column in the input worksheet, converting from Eastern to Pacific.
    """
    try:
        col_idx = list(input_records[0].keys()).index(time_col) + 1
        cell_list = input_ws.range(2, col_idx, len(input_records) + 1, col_idx)
        for i, cell in enumerate(cell_list):
            orig_time = input_records[i][time_col]
            new_time = convert_time_eastern_to_pacific(orig_time)
            if new_time:
                cell.value = new_time
        input_ws.update_cells(cell_list)
        logging.info("Time column updated successfully.")
    except Exception as e:
        logging.error(f"Failed to update Time column: {e}")
        raise


def delete_rows_up_to_datetime(input_ws: Worksheet, time_col: str, most_recent: datetime) -> None:
    """
    Delete all rows in the input worksheet up to and including the row with the most recent datetime.
    """
    try:
        input_records = input_ws.get_all_records()
        del_idx = None
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%b %d, %Y, %I:%M:%S %p",
            "%b %d, %Y, %I:%M %p"
        ]
        for i, row in enumerate(input_records):
            for fmt in formats:
                try:
                    dt = datetime.strptime(row[time_col], fmt)
                    if dt <= most_recent:
                        del_idx = i
                    break
                except Exception:
                    continue
        if del_idx is not None:
            input_ws.delete_rows(2, del_idx + 2)
            logging.info(f"Deleted rows 2 to {del_idx + 2} in input sheet.")
        else:
            logging.info("No rows to delete based on most recent timestamp.")
    except Exception as e:
        logging.error(f"Failed to delete rows: {e}")
        raise


def append_timestamps_and_extend_formula(
    input_ws: Worksheet,
    target_ws: Worksheet,
    input_time_col: str = 'Time',
    target_timestamp_col: str = 'Timestamp',
    expected_headers: Optional[List[str]] = None
) -> None:
    """
    Append time data from input_ws (input_time_col) to target_ws (target_timestamp_col) after the last timestamp entry.
    Extend the Delta formula for all added rows. Uses batch update to minimize API calls.
    Dynamically increments row numbers in the formula for each new row.
    """
    try:
        # Get input times
        input_records = input_ws.get_all_records()
        times = [r[input_time_col] for r in input_records if r.get(input_time_col)]
        num_to_add = len(times)
        if num_to_add == 0:
            logging.info("No times to append from input sheet.")
            return
        # Get target records and find last row with a timestamp
        if expected_headers:
            target_records = target_ws.get_all_records(expected_headers=expected_headers)
            num_cols = len(expected_headers)
        else:
            target_records = target_ws.get_all_records()
            num_cols = len(target_records[0].keys()) if target_records else 3
        last_row_idx = 1  # 1-based, header is row 1
        for i, row in enumerate(target_records, start=2):
            if row.get(target_timestamp_col):
                last_row_idx = i
        # Insert new rows after last timestamp row, with correct number of columns
        empty_row = [''] * num_cols
        target_ws.insert_rows([empty_row for _ in range(num_to_add)], row=last_row_idx + 1)
        # Prepare batch update for timestamps and formulas
        timestamp_col_idx = expected_headers.index(target_timestamp_col) if expected_headers else list(target_records[0].keys()).index(target_timestamp_col)
        delta_col_idx = expected_headers.index('Delta') if expected_headers else list(target_records[0].keys()).index('Delta')        # Get the formula from the last row with a Delta formula
        last_formula_cell = target_ws.cell(last_row_idx, delta_col_idx + 1)
        formula = last_formula_cell.value if last_formula_cell.value and str(last_formula_cell.value).startswith('=') else None
        logging.info(f"Original formula from row {last_row_idx}: {formula}")
        # Build new rows data
        new_rows = []
        for i, ts in enumerate(times):
            row = [''] * num_cols
            row[timestamp_col_idx] = ts
            if formula:
                # Increment row numbers in cell references (e.g., B10, C5) for each new row
                new_row_num = last_row_idx + 1 + i
                def increment_cell_references(match):
                    col_letter = match.group(1)
                    row_num = int(match.group(2))
                    # Increment the row number by the offset
                    new_row = row_num + 1 + i
                    return f"{col_letter}{new_row}"
                # Replace cell references like B10, C5, etc.
                new_formula = re.sub(r'([A-Z]+)(\d+)', increment_cell_references, formula)
                logging.info(f"New formula for row {new_row_num}: {new_formula}")
                row[delta_col_idx] = new_formula
            new_rows.append(row)
        # Batch update all new rows with correct argument order
        start_row = last_row_idx + 1
        end_row = start_row + num_to_add - 1
        range_name = f'A{start_row}:{chr(65+num_cols-1)}{end_row}'
        target_ws.update(range_name=range_name, values=new_rows)
        logging.info(f"Appended {num_to_add} times from input sheet and extended Delta formula (batch update).")
    except Exception as e:
        logging.error(f"Failed to append times and extend formula: {e}")
        raise


def main() -> None:
    """
    Main function to process the Google Sheets as described in the requirements.
    """
    parser = argparse.ArgumentParser(
        description="Convert Time column from US Eastern to US Pacific and clean rows in Google Sheets."
    )
    args = parser.parse_args()
    try:
        config = load_config()
        credentials_path = config['google']['credentials_json']
        sheet_name = config['google']['sheet_name']
        input_sheet_name = config['google']['input_sheet_name']

        client = get_gspread_client(credentials_path)

        # Open input spreadsheet and its Sheet1 tab
        input_sh = client.open(input_sheet_name)
        input_ws = input_sh.worksheet('Sheet1')

        # Open target spreadsheet and its Data tab
        target_sh = client.open(sheet_name)
        target_ws = target_sh.worksheet('Data')

        # Get all records from input sheet
        input_records = input_ws.get_all_records()
        if not input_records:
            logging.info("No records found in input sheet.")
            return

        # Find Time column
        time_col = None
        for key in input_records[0].keys():
            if key.lower() == 'time':
                time_col = key
                break
        if not time_col:
            logging.error("No 'Time' column found in input sheet.")
            return

        # Update Time column in batch
        update_time_column(input_ws, time_col, input_records)

        # Find most recent timestamp in target sheet, using explicit headers for blank column A
        expected_headers = ['', 'Timestamp', 'Delta']
        most_recent = get_most_recent_timestamp(target_ws, timestamp_col='Timestamp', expected_headers=expected_headers)
        if not most_recent:
            logging.info("No valid timestamps found in target sheet.")
            return

        # Re-fetch input records after update
        input_records = input_ws.get_all_records()

        # Delete rows up to and including most recent datetime in input sheet
        delete_rows_up_to_datetime(input_ws, time_col, most_recent)

        # Append times and extend Delta formula in target sheet
        append_timestamps_and_extend_formula(
            input_ws,
            target_ws,
            input_time_col=time_col,  # usually 'Time'
            target_timestamp_col='Timestamp',
            expected_headers=['', 'Timestamp', 'Delta']
        )
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")


if __name__ == '__main__':
    main()
