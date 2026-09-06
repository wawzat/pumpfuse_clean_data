"""Open the target Google Sheet configured in ``config.ini``.

Usage:
    python open_target_sheet.py
"""

import configparser
import logging
import webbrowser
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials


LOGGER = logging.getLogger(__name__)


def load_config(config_path: Path) -> configparser.SectionProxy:
    """Load and validate the Google Sheets settings."""
    config = configparser.ConfigParser()
    if not config.read(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        google_config = config["google"]
        google_config["credentials_json"]
        google_config["target_sheet_name"]
    except KeyError as error:
        raise KeyError(
            "config.ini must contain [google] credentials_json and "
            "target_sheet_name settings"
        ) from error

    return google_config


def open_target_sheet(config_path: Path = Path("config.ini")) -> None:
    """Open the configured target Google Sheet in the default web browser."""
    google_config = load_config(config_path)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        google_config["credentials_json"],
        scope,
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open(google_config["target_sheet_name"])

    if not webbrowser.open(spreadsheet.url):
        raise RuntimeError("The default web browser could not be opened")

    LOGGER.info("Opened target sheet: %s", google_config["target_sheet_name"])


def main() -> int:
    """Run the target-sheet opener and return a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        open_target_sheet()
    except KeyboardInterrupt:
        LOGGER.info("Operation cancelled.")
        return 130
    except (OSError, KeyError, RuntimeError, gspread.exceptions.GSpreadException) as error:
        LOGGER.error("Unable to open target sheet: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())