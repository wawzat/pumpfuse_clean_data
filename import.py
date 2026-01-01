"""
import.py

This script processes Google Sheets data for time zone conversion and data cleaning.

Features:
- Converts 'Time' values in the input sheet from US Eastern to US Pacific timezone, rounding to the nearest minute and formatting as 'YYYY-mm-dd h:m:00'.
- Deletes all rows in the input sheet up to and including the most recent datetime found in the 'Timestamp' column of the target sheet.
- Appends new timestamps from the input sheet to the target sheet and extends the 'Delta' formula for all new rows.
- Handles Google Sheets API authentication and configuration via config.ini.
- Logs all errors and key actions to 'clean_errors.log'.

Usage:
    python import.py

Requirements:
    - config.ini with Google Sheets and credentials configuration
    - See requirements.txt for dependencies
    - User must have access to the specified Google Sheets

Configuration:
    - config.ini for sensitive settings (API keys, sheet names, credentials)
    - user_settings.ini for user-specific non-sensitive settings

Logging:
    - All errors and info are logged to 'clean_errors.log'

Exception Handling:
    - Graceful handling of errors and KeyboardInterrupt (Ctrl-C)

"""

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


def format_timestamps(dt: datetime) -> str:
    """Format a datetime object, rounding to nearest minute, formatted as 'YYYY-mm-dd h:m:00'."""
    # Round to nearest minute
    if dt.second >= 30:
        dt = dt.replace(second=0) + timedelta(minutes=1)
    else:
        dt = dt.replace(second=0)
    # Format: YYYY-mm-dd h:m:00 (no leading zero for hour, cross-platform)
    formatted = dt.strftime('%Y-%m-%d %H:%M:00')
    # Remove leading zero from hour if present
    formatted = formatted.replace(' 0', ' ')
    return formatted


def parse_timestamp(time_str: str) -> Optional[datetime]:
    """Parse a time string into a datetime object, trying multiple formats."""
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%b %d, %Y, %I:%M:%S %p',
        '%b %d, %Y, %I:%M %p'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    logging.error(f"Unrecognized time format: {time_str}")
    return None


def convert_time_eastern_to_pacific(dt: datetime) -> datetime:
    """Convert a datetime object from US Eastern to US Pacific timezone."""
    eastern = pytz.timezone('US/Eastern')
    pacific = pytz.timezone('US/Pacific')
    dt_eastern = eastern.localize(dt)
    dt_pacific = dt_eastern.astimezone(pacific)
    return dt_pacific.replace(tzinfo=None)  # Return naive datetime for consistency


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
        for t in timestamps:
            dt = parse_timestamp(t)
            if dt:
                dt_list.append(dt)
        if not dt_list:
            return None
        return max(dt_list)
    except Exception as e:
        logging.error(f"Error finding most recent timestamp: {e}")
        return None


def update_time_column(input_ws: Worksheet, time_col: str, input_records: List[dict], convert_timezone: bool = False) -> None:
    """
    Batch update the Time column in the input worksheet.
    Always formats timestamps. Optionally converts from Eastern to Pacific timezone.
    """
    try:
        col_idx = list(input_records[0].keys()).index(time_col) + 1
        cell_list = input_ws.range(2, col_idx, len(input_records) + 1, col_idx)
        for i, cell in enumerate(cell_list):
            orig_time = input_records[i][time_col]
            # Parse timestamp once
            dt = parse_timestamp(orig_time)
            if not dt:
                continue
            # Optionally convert timezone
            if convert_timezone:
                dt = convert_time_eastern_to_pacific(dt)
            # Always format the timestamp
            formatted_time = format_timestamps(dt)
            cell.value = formatted_time
        input_ws.update_cells(cell_list)
        logging.info("Time column updated successfully.")
    except Exception as e:
        logging.error(f"Failed to update Time column: {e}")
        raise


def delete_rows_up_to_datetime(input_ws: Worksheet, time_col: str, most_recent: datetime) -> None:
    """
    Delete all rows in the input worksheet up to and including the last row 
    that matches the most recent datetime from the target sheet.
    """
    try:
        input_records = input_ws.get_all_records()
        del_idx = None
        for i, row in enumerate(input_records):
            dt = parse_timestamp(row[time_col])
            # Only update del_idx if we find an exact match
            if dt and dt == most_recent:
                del_idx = i
        
        if del_idx is not None:
            input_ws.delete_rows(2, del_idx + 2)
            logging.info(f"Deleted rows 2 to {del_idx + 2} in input sheet (up to and including timestamp: {most_recent}).")
        else:
            logging.info(f"No rows found matching the most recent timestamp ({most_recent}). No rows deleted.")
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
            headers = expected_headers
            target_records = target_ws.get_all_records(expected_headers=expected_headers)
            num_cols = len(expected_headers)
        else:
            headers = target_ws.row_values(1)
            if not headers:
                logging.warning("Target sheet has no headers, cannot proceed.")
                return
            target_records = target_ws.get_all_records()
            num_cols = len(headers)

        last_row_idx = 1  # 1-based, header is row 1
        for i, row in enumerate(target_records, start=2):
            if row.get(target_timestamp_col):
                last_row_idx = i
        
        # Insert new rows after last timestamp row, with correct number of columns
        empty_row = [''] * num_cols
        if num_to_add > 0:
            target_ws.insert_rows([empty_row for _ in range(num_to_add)], row=last_row_idx + 1)
        
        # Prepare batch update for timestamps and formulas
        timestamp_col_idx = headers.index(target_timestamp_col)
        delta_col_idx = headers.index('Delta')
        
        # Get the formula from the last row with a Delta formula by searching backwards
        formula = None
        formula_base_row = 0
        for i in range(last_row_idx, 1, -1):
            try:
                cell = target_ws.cell(i, delta_col_idx + 1, value_render_option='FORMULA')
                if cell.value and str(cell.value).startswith('='):
                    formula = cell.value
                    formula_base_row = i
                    break
            except Exception as e:
                logging.debug(f"Could not fetch cell ({i}, {delta_col_idx + 1}): {e}")
                continue # Ignore errors for cells that can't be fetched
        
        logging.info(f"Using formula from row {formula_base_row}: {formula}")

        # Build new rows data
        new_rows = []
        if formula:
            def get_new_formula(original_formula, base_row, current_row):
                offset = current_row - base_row
                def increment_row_references(match):
                    col = match.group(1)
                    row = int(match.group(2))
                    return f"{col}{row + offset}"
                return re.sub(r'([A-Z]+)(\d+)', increment_row_references, original_formula)

            for i, ts in enumerate(times):
                row = [''] * num_cols
                row[timestamp_col_idx] = ts
                new_row_num = last_row_idx + 1 + i
                new_formula = get_new_formula(formula, formula_base_row, new_row_num)
                row[delta_col_idx] = new_formula
                new_rows.append(row)
        else:
            logging.warning("No formula found to extend.")
            for ts in times:
                row = [''] * num_cols
                row[timestamp_col_idx] = ts
                new_rows.append(row)

        # Batch update all new rows with correct argument order
        if new_rows:
            start_row = last_row_idx + 1
            end_row = start_row + num_to_add - 1
            
            end_col_char = ''
            n = num_cols
            while n > 0:
                n, remainder = divmod(n - 1, 26)
                end_col_char = chr(65 + remainder) + end_col_char
            
            range_name = f'A{start_row}:{end_col_char}{end_row}'
            target_ws.update(range_name=range_name, values=new_rows, value_input_option='USER_ENTERED')
        logging.info(f"Appended {num_to_add} times from input sheet and extended Delta formula (batch update).")
    except Exception as e:
        logging.error(f"Failed to append times and extend formula: {e}")
        raise


def main() -> None:
    """
    Main function to process the Google Sheets as described in the requirements.
    """
    try:
        config = load_config()
        credentials_path = config['google']['credentials_json']
        target_sheet_name = config['google']['target_sheet_name']
        input_sheet_name = config['google']['input_sheet_name']
        convert_timezone = config.getboolean('optons', 'convert_timezone', fallback=False)

        client = get_gspread_client(credentials_path)

        # Open input spreadsheet and its Sheet1 tab
        input_sh = client.open(input_sheet_name)
        input_ws = input_sh.worksheet('Sheet1')

        # Open target spreadsheet and its Data tab
        target_sh = client.open(target_sheet_name)
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

        # Update Time column in batch (always format, optionally convert timezone)
        update_time_column(input_ws, time_col, input_records, convert_timezone=convert_timezone)
        if convert_timezone:
            logging.info("Timezone conversion enabled: converted timestamps from Eastern to Pacific.")
        else:
            logging.info("Timezone conversion disabled: timestamps formatted only.")

        # Find most recent timestamp in target sheet, using explicit headers for blank column A
        expected_headers = ['', 'Timestamp', 'Delta']
        most_recent = get_most_recent_timestamp(target_ws, timestamp_col='Timestamp', expected_headers=expected_headers)
        if not most_recent:
            logging.info("No valid timestamps found in target sheet.")
            return

        # Print the latest datetime value and its associated row number before importing data
        # Find the row number for the most recent timestamp
        target_records = target_ws.get_all_records(expected_headers=expected_headers)
        row_number = None
        for idx, row in enumerate(target_records, start=2):  # Data starts at row 2
            ts = row.get('Timestamp')
            if not ts:
                continue
            dt = parse_timestamp(ts)
            if dt and dt == most_recent:
                row_number = idx
                break
        print(f"Latest datetime in 'Timestamp' column: {most_recent} (row {row_number})")

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
