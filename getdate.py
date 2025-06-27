"""
getdate.py

This script retrieves the most recent date from the Google Sheet specified in the config.ini file (under the variable 'sheet_name') and prints it to the console.

Usage:
    python getdate.py

Requirements:
    - config.ini with Google Sheets and credentials configuration
    - See requirements.txt for dependencies
    - User must have access to the specified Google Sheets

Command Line Arguments:
    -h, --help      Show usage instructions

Configuration:
    - config.ini for sensitive settings (API keys, sheet names, credentials)
    - user_settings.ini for user-specific non-sensitive settings

Logging:
    - All errors and info are logged to 'clean_errors.log'

Exception Handling:
    - Graceful handling of errors and KeyboardInterrupt (Ctrl-C)

"""

import argparse
import configparser
import logging
from datetime import datetime
from typing import List, Optional
import gspread
from gspread.worksheet import Worksheet
from oauth2client.service_account import ServiceAccountCredentials

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

def main() -> None:
    """
    Main function to retrieve and print the most recent date from the configured Google Sheet.
    """
    parser = argparse.ArgumentParser(
        description="Get the most recent date from the Google Sheet specified in config.ini (sheet_name)."
    )
    args = parser.parse_args()
    try:
        config = load_config()
        credentials_path = config['google']['credentials_json']
        sheet_name = config['google']['sheet_name']
        # Open the spreadsheet and Data worksheet
        client = get_gspread_client(credentials_path)
        sh = client.open(sheet_name)
        ws = sh.worksheet('Data')
        # Use explicit headers for blank column A
        expected_headers = ['', 'Timestamp', 'Delta']
        most_recent = get_most_recent_timestamp(ws, timestamp_col='Timestamp', expected_headers=expected_headers)
        if most_recent:
            print(f"Latest date in {sheet_name} is: {most_recent}")
        else:
            print(f"No valid dates found in {sheet_name}.")
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
