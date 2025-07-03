"""
Module to automate Looker Studio date range selection using Selenium.

This script demonstrates how to open the date range selector and select a start date based on the latest datetime from a Google Sheet.
"""

import configparser
import logging
from typing import Optional
from datetime import datetime
import gspread
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

def get_latest_datetime_from_sheet(config_path: str = "config.ini") -> Optional[datetime]:
    """
    Retrieves the latest datetime from the target Google Sheet specified in config.ini.

    Args:
        config_path (str): Path to the config.ini file.

    Returns:
        Optional[datetime]: The latest datetime found in the sheet, or None if not found.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    credentials_json = config["google"]["credentials_json"]
    target_sheet_name = config["google"]["target_sheet_name"]

    gc = gspread.service_account(filename=credentials_json)
    sh = gc.open(target_sheet_name)
    worksheet = sh.sheet1  # Adjust if not the first sheet
    # Use explicit headers to avoid duplicate header issues (adjust as needed)
    expected_headers = ['', 'Timestamp', 'Delta']
    try:
        records = worksheet.get_all_records(expected_headers=expected_headers)
        headers = expected_headers
        # Check for duplicate headers (allow a single empty string)
        header_counts = {}
        for h in headers:
            header_counts[h] = header_counts.get(h, 0) + 1
        duplicates = [k for k, v in header_counts.items() if v > 1]
        if duplicates:
            logging.error(f"Header row contains duplicate values: {headers}")
            return None
        # Find the latest datetime in the 'Timestamp' column
        timestamps = [r['Timestamp'] for r in records if r.get('Timestamp')]
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
            logging.error("No valid datetime found in the latest row.")
            return None
        return max(dt_list)
    except Exception as e:
        logging.error(f"Error retrieving latest datetime: {e}")
        return None

def select_looker_date_range(driver: webdriver.Edge, start_day: int, timeout: int = 10) -> bool:
    """
    Opens the Looker Studio date range selector and selects the given start date.

    Args:
        driver (webdriver.Edge): Selenium WebDriver instance.
        start_day (int): The day of the month to select as the start date.
        timeout (int): Maximum time to wait for elements (in seconds).

    Returns:
        bool: True if the date was selected successfully, False otherwise.
    """
    try:
        # Wait for the date range selector to be clickable and click it
        wait = WebDriverWait(driver, timeout)
        date_selector = wait.until(
            EC.element_to_be_clickable((By.CLASS_NAME, "date-text"))
        )
        date_selector.click()
        logging.info("Clicked date range selector.")

        # Wait for the calendar popup to appear
        calendar_popup = wait.until(
            EC.visibility_of_element_located((By.CLASS_NAME, "mat-calendar"))
        )
        logging.info("Calendar popup is visible.")

        # Find the start date cell by its text (day of month)
        day_xpath = f"//span[contains(@class, 'mat-calendar-body-cell-content') and normalize-space(text())='{start_day}']"
        start_date_cell = wait.until(
            EC.element_to_be_clickable((By.XPATH, day_xpath))
        )
        start_date_cell.click()
        logging.info(f"Selected start date: {start_day}")

        return True

    except (TimeoutException, NoSuchElementException, WebDriverException) as e:
        logging.error(f"Error selecting date: {e}")
        return False

if __name__ == "__main__":
    import argparse
    import sys
    config = configparser.ConfigParser()
    config.read("config.ini")
    looker_url = config["looker"]["report_url"]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    # Get latest datetime from Google Sheet
    latest_dt = get_latest_datetime_from_sheet()
    if not latest_dt:
        logging.error("Could not retrieve latest datetime from Google Sheet.")
        sys.exit(1)
    start_day = latest_dt.day
    logging.info(f"Using start day from Google Sheet: {start_day}")

    driver: Optional[webdriver.Edge] = None
    try:
        driver = webdriver.Edge()
        driver.get(looker_url)
        logging.info(f"Opened URL: {looker_url}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "date-text"))
        )

        if select_looker_date_range(driver, start_day):
            logging.info("Date selection completed successfully.")
        else:
            logging.error("Date selection failed.")

    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if driver:
            logging.info("Leaving Looker Studio page open for user inspection. Close the browser window manually when done.")
            try:
                while True:
                    input("Press Ctrl+C in this terminal to close the browser and exit the script...\n")
            except KeyboardInterrupt:
                logging.info("User requested shutdown. Closing browser.")
                driver.quit()