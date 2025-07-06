"""
getweather.py

Fetches historical weather data (temperature, humidity, precipitation) from Open-Meteo for each row in the target Google Sheet,
starting from a specified row or auto-detecting the next row to process if not specified. Weather data is matched to each timestamp by nearest hour and written back to the sheet.

Usage:
    python getweather.py [--start-row N]

Requirements:
    - config.ini with Google Sheets and credentials configuration
    - See requirements.txt for dependencies
    - User must have access to the specified Google Sheets

Command Line Arguments:
    --start-row, -s   Optional row number to start processing (1-based, header is row 1). If not provided, the script will auto-detect the next row to process.
    -h, --help        Show usage instructions

Configuration:
    - config.ini for sensitive settings (API keys, sheet names, credentials)
    - user_settings.ini for user-specific non-sensitive settings

Logging:
    - All errors and info are logged to 'clean_errors.log'
"""

import argparse
import configparser
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
import gspread
from gspread.worksheet import Worksheet
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

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


def get_coordinates_from_config(config: configparser.ConfigParser) -> tuple[float, float]:
    """Read latitude and longitude from config.ini [weather] section."""
    try:
        latitude = float(config.get('weather', 'latitude'))
        longitude = float(config.get('weather', 'longitude'))
        return latitude, longitude
    except Exception as e:
        logging.error(f"Could not read latitude/longitude from config.ini: {e}")
        raise


def get_gspread_client(credentials_path: str) -> gspread.Client:
    """Authenticate and return a gspread client."""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    return gspread.authorize(creds)


def fetch_weather_data(start: datetime, end: datetime, latitude: float, longitude: float) -> pd.DataFrame:
    """
    Fetch hourly weather data from Open-Meteo for the given date range and location.
    Returns a DataFrame indexed by datetime. Temperature is converted to Fahrenheit.
    """
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={latitude}&longitude={longitude}"
        f"&start_date={start.date()}&end_date={end.date()}"
        "&hourly=temperature_2m,relative_humidity_2m,precipitation"
        "&timezone=UTC"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        times = data['hourly']['time']
        temp_c = data['hourly']['temperature_2m']
        temp_f = [(t * 9/5) + 32 if t is not None else None for t in temp_c]
        rh = data['hourly']['relative_humidity_2m']
        precip_mm = data['hourly']['precipitation']
        # Convert precipitation from mm to inches (1 mm = 0.0393701 in)
        precip_in = [round(p * 0.0393701, 3) if p is not None else None for p in precip_mm]
        df = pd.DataFrame({
            'datetime': pd.to_datetime(times),
            'Temperature (F)': temp_f,
            'Humidity (%)': rh,
            'Precipitation (in)': precip_in
        }).set_index('datetime')
        return df
    except Exception as e:
        logging.error(f"Error fetching weather data: {e}")
        raise


def get_sheet_data(ws: Worksheet, start_row: int) -> List[Dict[str, Any]]:
    """
    Get all records from the worksheet starting from the specified row (1-based).
    """
    all_values = ws.get_all_values()
    headers = all_values[0]
    # Only skip header row, do not filter out empty rows at the end
    data_rows = all_values[start_row-1:]
    # Pad rows to header length to avoid missing columns
    padded_rows = [row + [''] * (len(headers) - len(row)) for row in data_rows]
    # Keep all rows, even if empty, so that the last row is always included
    records = [dict(zip(headers, row)) for row in padded_rows]
    return records


def parse_timestamp(ts: str) -> Optional[datetime]:
    """Try to parse a timestamp string into a datetime object."""
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%b %d, %Y, %I:%M:%S %p',
        '%b %d, %Y, %I:%M %p'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return None


def ensure_weather_columns(ws: Worksheet, headers: List[str]) -> None:
    """
    Ensure the worksheet has correct weather column headers in columns D, E, F; do not insert columns, just update headers if needed.
    """
    weather_cols = ['Precipitation (in)', 'Temperature (F)', 'Humidity (%)']
    # Only update headers in columns D, E, F (4, 5, 6)
    headers_updated = False
    for idx, col in enumerate(weather_cols, start=4):
        if len(headers) < idx:
            # If headers are too short, pad them
            headers += [''] * (idx - len(headers) + 1)
        if headers[idx-1] != col:
            ws.update_cell(1, idx, col)
            headers_updated = True
            logging.info(f"Set weather column header: {col} at position {idx}")
    if headers_updated:
        logging.info("Weather column headers updated in columns D-F.")


def update_sheet_with_weather(ws: Worksheet, start_row: int, weather_results: List[Dict[str, Any]], headers: List[str]) -> None:
    """
    Batch update the worksheet with weather data starting at the given row.
    Precipitation (mm) in E, Temperature (F) in F, Humidity (%) in G.
    """
    # Prepare a 2D list for columns E, F, G
    values = [
        [w['Precipitation (in)'], w['Temperature (F)'], w['Humidity (%)']]
        for w in weather_results
    ]
    end_row = start_row + len(values) - 1
    try:
        ws.update(values, f'D{start_row}:F{end_row}')
        logging.info(f"Batch updated weather data in range D{start_row}:F{end_row}")
    except Exception as e:
        logging.error(f"Batch update failed for range D{start_row}:F{end_row}: {e}")


def main() -> None:
    """
    Main function to fetch and add weather data to the Google Sheet.
    """
    parser = argparse.ArgumentParser(
        description="Add historical weather data to Google Sheet rows using Open-Meteo."
    )
    parser.add_argument(
        '--start-row', '-s', type=int, required=False,
        help="Row number to start processing (1-based, header is row 1). If not provided, will auto-detect."
    )
    args = parser.parse_args()
    try:
        config = load_config()

        credentials_path = config['google']['credentials_json']
        target_sheet_name = config['google']['target_sheet_name']
        latitude, longitude = get_coordinates_from_config(config)

        client = get_gspread_client(credentials_path)
        sh = client.open(target_sheet_name)
        ws = sh.worksheet('Data')

        all_values = ws.get_all_values()
        headers = all_values[0]
        ensure_weather_columns(ws, headers)
        # Refresh headers in case columns were added
        headers = ws.get_all_values()[0]

        # Determine start_row if not provided
        start_row = args.start_row
        if start_row is None:
            # Find the last row with weather data in all three columns (E, F, G)
            weather_cols_idx = [4, 5, 6]  # E, F, G (0-based)
            last_weather_row = 1  # header is row 1
            for idx, row in enumerate(all_values[1:], start=2):
                if all(len(row) > col and row[col].strip() for col in weather_cols_idx):
                    last_weather_row = idx
            start_row = last_weather_row + 1
            if start_row > len(all_values):
                print("All rows already have weather data. Nothing to do.")
                logging.info("No new rows to process; all rows have weather data.")
                return

        records = get_sheet_data(ws, start_row)
        # Remove trailing empty records (all fields empty)
        def is_empty_record(rec: dict) -> bool:
            return all((v is None or str(v).strip() == '') for v in rec.values())
        # Only drop trailing empty records, not those in the middle
        while records and is_empty_record(records[-1]):
            records.pop()
        if not records:
            print("No new rows to process.")
            logging.info(f"No new rows to process from start_row {start_row}.")
            return
        timestamps = [parse_timestamp(r.get('Timestamp', '')) for r in records]
        timestamps = [t for t in timestamps if t is not None]
        if not timestamps:
            logging.error("No valid timestamps found in the specified rows.")
            print("No valid timestamps found.")
            return

        start_dt = min(timestamps)
        end_dt = max(timestamps)
        weather_df = fetch_weather_data(start_dt, end_dt, latitude, longitude)

        # For each record, find the nearest hour in weather_df
        weather_results = []
        for r in records:
            ts = parse_timestamp(r.get('Timestamp', ''))
            if ts is None:
                weather_results.append({'Precipitation (in)': None, 'Temperature (F)': None, 'Humidity (%)': None})
                continue
            nearest = weather_df.index.get_indexer([ts], method='nearest')[0]
            weather_row = weather_df.iloc[nearest]
            weather_results.append({
                'Precipitation (in)': weather_row['Precipitation (in)'],
                'Temperature (F)': weather_row['Temperature (F)'],
                'Humidity (%)': weather_row['Humidity (%)']
            })

        update_sheet_with_weather(ws, start_row, weather_results, headers)
        print(f"Weather data added to rows {start_row} to {start_row + len(weather_results) - 1}.")

    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
