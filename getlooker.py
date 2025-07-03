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
    Opens the Looker Studio date range selector and selects the given start date, then clicks the Apply button.

    Args:
        driver (webdriver.Edge): Selenium WebDriver instance.
        start_day (int): The day of the month to select as the start date.
        timeout (int): Maximum time to wait for elements (in seconds).

    Returns:
        bool: True if the date was selected and applied successfully, False otherwise.
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

        # Wait for and click the Apply button
        apply_xpath = "//button[.//span[normalize-space(text())='Apply']] | //span[normalize-space(text())='Apply']"
        try:
            apply_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, apply_xpath))
            )
            apply_button.click()
            logging.info("Clicked Apply button.")
        except TimeoutException:
            logging.error("Apply button not found or not clickable.")
            return False

        return True

    except (TimeoutException, NoSuchElementException, WebDriverException) as e:
        logging.error(f"Error selecting date: {e}")
        return False

def export_data_to_google_sheets(driver: webdriver.Edge, timeout: int = 20) -> bool:
    """
    Automates the export of data to Google Sheets via the Looker Studio UI.

    Steps:
    1. Wait for the first data row to appear (after date selection).
    2. Right-click the first data row to open the context menu.
    3. Click the Export option.
    4. Change the export name to 'PumpFuse_new'.
    5. Select the Google Sheets radio button.
    6. Click the Export button.

    Args:
        driver (webdriver.Edge): Selenium WebDriver instance.
        timeout (int): Maximum time to wait for elements (in seconds).

    Returns:
        bool: True if export was successful, False otherwise.
    """
    from selenium.webdriver.common.action_chains import ActionChains
    import time
    try:
        wait = WebDriverWait(driver, timeout)
        actions = ActionChains(driver)

        # 1. Wait for the first data row to appear (after date selection)
        data_selector = ".centerColsContainer .row.block-0.index-0"
        logging.info("Waiting for first data row to appear after date selection...")
        data_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, data_selector))
        )
        time.sleep(1)  # Small buffer to ensure table is interactive
        actions.context_click(data_element).perform()
        logging.info("Right-clicked first data row to open context menu.")

        # 2. Click the Export option in the context menu (by data-test-id)
        export_option_xpath = "//button[@data-test-id='Export']"
        export_option = wait.until(
            EC.element_to_be_clickable((By.XPATH, export_option_xpath))
        )
        export_option.click()
        logging.info("Clicked Export option in context menu.")

        # 3. Change the export name to 'PumpFuse_new' using the export-name-field class
        name_input_css = "input.export-name-field"
        name_input = wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, name_input_css))
        )
        name_input.clear()
        name_input.send_keys("PumpFuse_new")
        logging.info("Changed export name to PumpFuse_new.")

        # 4. Select the Google Sheets radio button robustly
        try:
            # Try to find the label and click the associated input by id
            label_xpath = "//label[contains(., 'Google Sheets')]"
            label_elem = wait.until(
                EC.presence_of_element_located((By.XPATH, label_xpath))
            )
            radio_id = label_elem.get_attribute("for")
            if radio_id:
                radio_input = driver.find_element(By.ID, radio_id)
                driver.execute_script("arguments[0].scrollIntoView(true);", radio_input)
                if not radio_input.is_selected():
                    radio_input.click()
                    logging.info("Selected Google Sheets radio button via input id.")
                else:
                    logging.info("Google Sheets radio button already selected.")
            else:
                # Fallback: click the parent radio button
                parent_radio = label_elem.find_element(By.XPATH, "ancestor::mat-radio-button")
                driver.execute_script("arguments[0].scrollIntoView(true);", parent_radio)
                parent_radio.click()
                logging.info("Selected Google Sheets radio button via parent mat-radio-button.")
        except Exception as e:
            logging.error(f"Could not select Google Sheets radio button: {e}")
            return False

        # 5. Click the Export button (look for button with span containing 'Export')
        try:
            export_button_xpath = "//button[.//span[contains(text(),'Export')]]"
            export_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, export_button_xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", export_button)
            export_button.click()
            logging.info("Clicked Export button to complete export.")
        except Exception as e:
            logging.error(f"Could not click Export button: {e}")
            return False

        return True
    except Exception as e:
        logging.error(f"Error during export to Google Sheets: {e}")
        return False

if __name__ == "__main__":
    import argparse
    import sys
    config = configparser.ConfigParser()
    config.read("config.ini")
    looker_url = config["looker"]["report_url"]
    windows_username = config.get("windows", "username", fallback=None)
    profile_directory = config.get("windows", "profile_directory", fallback="Default")
    if not windows_username:
        logging.error("No Windows username found in config.ini under [windows] section.")
        sys.exit(1)
    edge_user_data_dir = fr"C:\\Users\\{windows_username}\\AppData\\Local\\Microsoft\\Edge\\User Data"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    # Reduce noisy urllib3/selenium warnings after shutdown
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
    logging.getLogger("selenium.webdriver.remote.remote_connection").setLevel(logging.ERROR)

    # Get latest datetime from Google Sheet
    latest_dt = get_latest_datetime_from_sheet()
    if not latest_dt:
        logging.error("Could not retrieve latest datetime from Google Sheet.")
        sys.exit(1)
    start_day = latest_dt.day
    logging.info(f"Using start day from Google Sheet: {start_day}")

    driver: Optional[webdriver.Edge] = None
    try:
        from selenium.webdriver.edge.options import Options
        edge_options = Options()
        edge_options.add_argument(fr"--user-data-dir={edge_user_data_dir}")
        edge_options.add_argument(f"--profile-directory={profile_directory}")
        driver = webdriver.Edge(options=edge_options)
        driver.get(looker_url)
        logging.info(f"Opened URL: {looker_url}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "date-text"))
        )

        if select_looker_date_range(driver, start_day):
            logging.info("Date selection completed successfully.")
            if export_data_to_google_sheets(driver):
                logging.info("Export to Google Sheets completed successfully.")
            else:
                logging.error("Export to Google Sheets failed.")
        else:
            logging.error("Date selection failed.")

    except KeyboardInterrupt:
        logging.info("Script interrupted by user. Closing browser.")
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logging.debug(f"Suppressed error during driver.quit(): {e}")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Only enter the input loop if not interrupted by KeyboardInterrupt
        if driver:
            try:
                while True:
                    input("Press Ctrl+C in this terminal to close the browser and exit the script...\n")
            except KeyboardInterrupt:
                logging.info("User requested shutdown. Closing browser.")
                try:
                    driver.quit()
                except Exception as e:
                    logging.debug(f"Suppressed error during driver.quit(): {e}")