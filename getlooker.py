"""
Automates Looker Studio date range selection, data export to Google Sheets, and Google Sheet sharing using Selenium.

This script:
- Retrieves the latest datetime from a target Google Sheet (using gspread and config.ini for credentials).
- Opens Looker Studio in Edge, selects a date range based on the latest sheet data, and exports the data to a new Google Sheet.
- Switches to the exported Google Sheet tab and shares it with a service account, ensuring the 'Notify people' checkbox is unchecked.
- Handles Google Sheets' and Looker Studio's dynamic UIs, including robust element selection and exception handling.
- Provides logging for all major steps and errors, and supports graceful shutdown on Ctrl+C.

Configuration, credentials, and user-specific settings are managed via config.ini.
"""

import configparser
import logging
from typing import Optional
from datetime import datetime
import json
import os
import gspread
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.keys import Keys

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

        # Determine if the start date is in the previous month
        # Get the currently displayed month and year from the calendar header
        header_xpath = "//div[contains(@class, 'mat-calendar-header')]//button[contains(@class, 'mat-calendar-period-button')]/span"
        header_elem = wait.until(EC.visibility_of_element_located((By.XPATH, header_xpath)))
        displayed_month_year = header_elem.text.strip()
        # Parse displayed month and year
        from datetime import datetime
        try:
            displayed_month_dt = datetime.strptime(displayed_month_year, "%b %Y")
        except ValueError:
            logging.warning(f"Could not parse calendar header: {displayed_month_year}")
            displayed_month_dt = datetime.now()

        # Get today's date and calculate previous month
        today = datetime.today()
        prev_month = today.month - 1 if today.month > 1 else 12
        prev_year = today.year if today.month > 1 else today.year - 1

        # If the start_day is greater than today.day, assume previous month
        if start_day > today.day:
            # If displayed month is not previous month, click previous month button
            if displayed_month_dt.month != prev_month or displayed_month_dt.year != prev_year:
                try:
                    prev_btn_xpath = "//button[contains(@class, 'mat-calendar-previous-button') and @aria-label='Previous month']"
                    prev_btn = wait.until(EC.element_to_be_clickable((By.XPATH, prev_btn_xpath)))
                    prev_btn.click()
                    logging.info("Clicked previous month button.")
                except TimeoutException:
                    logging.warning("Previous month button not found or not clickable.")

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
    3. Click the Export data option.
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
        # Wait for the element to be visible and interactable
        time.sleep(2)  # Increased buffer to ensure table is fully loaded
        
        # Scroll the element into view before right-clicking
        driver.execute_script("arguments[0].scrollIntoView(true);", data_element)
        time.sleep(1)
        
        actions.context_click(data_element).perform()
        logging.info("Right-clicked first data row to open context menu.")
        
        # Add explicit wait for context menu to appear
        time.sleep(1)

        # 2. Click the "Export chart..." option in the context menu
        # Use shorter timeout when trying multiple selectors
        short_wait = WebDriverWait(driver, 5)
        export_chart_option = None
        try:
            # First attempt: text content (known to work based on logs)
            export_chart_xpath = "//button[contains(., 'Export chart')]"
            export_chart_option = short_wait.until(
                EC.element_to_be_clickable((By.XPATH, export_chart_xpath))
            )
        except TimeoutException:
            logging.info("Export chart option not found by text content, trying data-test-id...")
            try:
                # Second attempt: data-test-id as fallback
                export_chart_xpath = "//button[@data-test-id='Export chart']"
                export_chart_option = short_wait.until(
                    EC.element_to_be_clickable((By.XPATH, export_chart_xpath))
                )
            except TimeoutException:
                logging.error("Export chart option not found in context menu. Menu may not have appeared.")
                # Take a screenshot for debugging
                try:
                    driver.save_screenshot("context_menu_error.png")
                    logging.info("Screenshot saved as context_menu_error.png for debugging.")
                except Exception:
                    pass
                return False
        
        export_chart_option.click()
        logging.info("Clicked 'Export chart...' option in context menu.")
        
        # Wait for the fly-out dialog to appear
        time.sleep(1)
        
        # 2b. Click the "Export data" option in the fly-out dialog
        export_data_option = None
        try:
            # First attempt: data-test-id with "Export data"
            export_data_xpath = "//button[@data-test-id='Export data']"
            export_data_option = short_wait.until(
                EC.element_to_be_clickable((By.XPATH, export_data_xpath))
            )
        except TimeoutException:
            logging.info("Export data option not found by data-test-id, trying text content...")
            try:
                # Second attempt: text content with "Export data"
                export_data_xpath = "//button[contains(., 'Export data')]"
                export_data_option = short_wait.until(
                    EC.element_to_be_clickable((By.XPATH, export_data_xpath))
                )
            except TimeoutException:
                logging.error("Export data option not found in fly-out menu.")
                # Take a screenshot for debugging
                try:
                    driver.save_screenshot("export_data_menu_error.png")
                    logging.info("Screenshot saved as export_data_menu_error.png for debugging.")
                except Exception:
                    pass
                return False
        
        export_data_option.click()
        logging.info("Clicked 'Export data' option in fly-out menu.")

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
        # Take a screenshot for debugging
        try:
            driver.save_screenshot("export_error.png")
            logging.info("Screenshot saved as export_error.png for debugging.")
        except Exception:
            pass
        return False

def share_google_sheet_with_service_account(driver: webdriver.Edge, config_path: str = "config.ini", timeout: int = 30) -> bool:
    """
    Shares the currently open Google Sheet with the service account email and unchecks the 'Notify people' checkbox.

    Args:
        driver (webdriver.Edge): Selenium WebDriver instance with the sheet open.
        config_path (str): Path to the config.ini file.
        timeout (int): Maximum time to wait for elements (in seconds).

    Returns:
        bool: True if sharing was successful, False otherwise.
    """
    import time
    config = configparser.ConfigParser()
    config.read(config_path)
    email = config.get('google', 'SERVICE_ACCOUNT_USER_EMAIL')
    wait = WebDriverWait(driver, timeout)
    try:
        driver.switch_to.window(driver.window_handles[-1])
        share_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@role='button' and contains(@aria-label, 'Share')]")))
        driver.execute_script("arguments[0].scrollIntoView(true);", share_btn)
        share_btn.click()
        logging.info("Clicked Share button.")
        time.sleep(3)  # Increased wait time for dialog to fully render

        # Wait for the share dialog iframe to be present and switch to it
        try:
            iframe = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "share-client-content-iframe")))
            driver.switch_to.frame(iframe)
            logging.info("Switched to share dialog iframe.")
            time.sleep(1)
        except TimeoutException:
            logging.warning("Share dialog iframe not found, attempting to find email input without iframe switch.")

        # Try multiple possible selectors for the email input (within the iframe context now)
        # Use shorter timeout when trying multiple selectors
        # Reordered based on logs: aria-label worked first time
        short_wait = WebDriverWait(driver, 3)
        email_input = None
        email_input_selectors = [
            "//input[contains(@aria-label, 'Add people')]",  # Known to work
            "//input[contains(@placeholder, 'Add people')]",
            "//input[contains(@placeholder, 'Add people and groups')]",
            "//input[contains(@placeholder, 'Add people, groups, spaces, and calendar events')]",
            "//input[@type='text']"
        ]
        
        for selector in email_input_selectors:
            try:
                logging.info(f"Trying email input selector: {selector}")
                email_input = short_wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                logging.info(f"Found email input using selector: {selector}")
                break
            except TimeoutException:
                continue
        
        if not email_input:
            logging.error("Could not find email input field with any known selector.")
            driver.save_screenshot("share_email_input_not_found.png")
            logging.info("Screenshot saved as share_email_input_not_found.png")
            return False

        driver.execute_script("arguments[0].scrollIntoView(true);", email_input)
        time.sleep(0.5)  # Ensure scroll completes
        
        # Use JavaScript to click and focus the input to avoid "element click intercepted" errors
        # This bypasses overlay elements that might intercept Selenium clicks
        driver.execute_script("arguments[0].click(); arguments[0].focus();", email_input)
        time.sleep(0.5)
        
        # Clear any existing text using keyboard shortcut
        email_input.send_keys(Keys.CONTROL + 'a')
        email_input.send_keys(Keys.DELETE)
        time.sleep(0.2)
        
        # Type email with incremental delays to ensure proper input
        for char in email:
            email_input.send_keys(char)
            time.sleep(0.05)
        
        logging.info(f"Entered email: {email}")
        time.sleep(2)  # Wait for autocomplete suggestions to appear
        email_input.send_keys(Keys.ARROW_DOWN)  # Select first autocomplete suggestion if available
        time.sleep(0.5)
        email_input.send_keys(Keys.ENTER)
        logging.info("Pressed Enter to confirm email selection.")
        
        # Wait for the notification dialog to appear
        time.sleep(3)  # Increased wait time
        
        # Try to find and uncheck the 'Notify people' checkbox (within iframe)
        try:
            notify_checkbox = None
            checkbox_selectors = [
                "//input[@type='checkbox' and @name='notify']",
                "//input[@type='checkbox' and contains(@aria-label, 'Notify')]",
                "//span[contains(text(), 'Notify people')]/ancestor::div[contains(@class, 'checkbox')]//input[@type='checkbox']",
                "//div[contains(text(), 'Notify people')]/preceding::input[@type='checkbox'][1]",
                "//input[@type='checkbox']"
            ]
            
            for selector in checkbox_selectors:
                try:
                    notify_checkbox = short_wait.until(EC.presence_of_element_located((By.XPATH, selector)))
                    logging.info(f"Found Notify people checkbox using selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if notify_checkbox:
                driver.execute_script("arguments[0].scrollIntoView(true);", notify_checkbox)
                if notify_checkbox.is_selected():
                    notify_checkbox.click()
                    logging.info("Unchecked Notify people checkbox.")
                    time.sleep(2)  # Increased wait for button text to change
                else:
                    logging.info("Notify people checkbox already unchecked.")
            else:
                logging.warning("Notify people checkbox not found with any selector.")
        except Exception as e:
            logging.warning(f"Could not interact with Notify people checkbox: {e}")
        
        # Click the Share, Send, or Done button (within iframe)
        # Use shorter timeout and try Share first (known to work based on logs)
        try:
            share_button = short_wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Share']]")))
            driver.execute_script("arguments[0].scrollIntoView(true);", share_button)
            share_button.click()
            logging.info("Clicked Share button in notification dialog.")
        except TimeoutException:
            try:
                send_button = short_wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Send']]")))
                driver.execute_script("arguments[0].scrollIntoView(true);", send_button)
                send_button.click()
                logging.info("Clicked Send button in notification dialog.")
            except TimeoutException:
                try:
                    share_button = short_wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Done']]")))
                    driver.execute_script("arguments[0].scrollIntoView(true);", share_button)
                    share_button.click()
                    logging.info("Clicked Done button.")
                except TimeoutException:
                    try:
                        button = short_wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Done') or contains(text(), 'Share') or contains(text(), 'Send')]")))
                        driver.execute_script("arguments[0].scrollIntoView(true);", button)
                        button.click()
                        logging.info("Clicked done/share/send button (fallback selector).")
                    except Exception as e:
                        logging.error(f"Could not find Done/Share/Send button: {e}")
                        driver.save_screenshot("share_dialog_error.png")
                        logging.info("Screenshot saved as share_dialog_error.png for debugging.")
                        return False
        
        # Wait for dialog to close and switch back to main content
        try:
            wait.until(EC.invisibility_of_element_located((By.XPATH, "//input[contains(@aria-label, 'Add people')]")))
            logging.info(f"Shared Google Sheet with {email} (notify people unchecked).")
        except TimeoutException:
            logging.warning("Share dialog did not close as expected, but operation may have succeeded.")
        
        driver.switch_to.default_content()
        return True
    except Exception as e:
        logging.error(f"Failed to share Google Sheet: {e}")
        driver.save_screenshot("share_error.png")
        logging.info("Screenshot saved as share_error.png for debugging.")
        driver.switch_to.default_content()
        return False

def wait_for_google_sheet_ready(driver: webdriver.Edge, timeout: int = 60) -> bool:
    """
    Waits for the Google Sheet to be fully loaded by waiting for the Share button to be clickable.

    Args:
        driver (webdriver.Edge): Selenium WebDriver instance with the sheet tab active.
        timeout (int): Maximum time to wait for the sheet to load (in seconds).

    Returns:
        bool: True if the sheet is ready, False otherwise.
    """
    try:
        WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@role='button' and contains(@aria-label, 'Share')]"))
        )
        return True
    except Exception as e:
        logging.error(f"Google Sheet did not become ready in time: {e}")
        return False

def switch_to_sheet_tab_by_title(driver: webdriver.Edge, sheet_title: str = "PumpFuse_new", timeout: int = 60) -> bool:
    """
    Switches to the browser tab whose title contains the given sheet_title.

    Args:
        driver (webdriver.Edge): Selenium WebDriver instance.
        sheet_title (str): Substring to look for in the tab title.
        timeout (int): Maximum time to wait for the tab to appear (in seconds).

    Returns:
        bool: True if switched successfully, False otherwise.
    """
    import time
    end_time = time.time() + timeout
    while time.time() < end_time:
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
            try:
                if sheet_title.lower() in driver.title.lower():
                    return True
            except Exception:
                continue
        time.sleep(2)
    return False

def resolve_edge_profile_directory(
    user_data_dir: str,
    config: configparser.ConfigParser,
    default_directory: str = "Default",
) -> str:
    """
    Resolves the Edge profile directory to use based on config.ini.

    This function supports two config options under the [edge] section:
    - profile_name: A friendly profile name shown in Edge (e.g., "Personal").
    - profile_directory: The profile directory name (e.g., "Default", "Profile 1").

    If profile_name is provided, the function attempts to map it to the correct
    directory using Edge's Local State file. If resolution fails, it falls back
    to profile_directory, then to the provided default_directory.

    Args:
        user_data_dir (str): Path to the Edge user data directory.
        config (configparser.ConfigParser): Loaded config instance.
        default_directory (str): Fallback profile directory name.

    Returns:
        str: The resolved Edge profile directory name.
    """
    profile_name = config.get("edge", "profile_name", fallback="").strip()
    profile_directory = config.get("edge", "profile_directory", fallback="").strip()

    if profile_name:
        local_state_path = os.path.join(user_data_dir, "Local State")
        try:
            with open(local_state_path, "r", encoding="utf-8") as local_state_file:
                local_state = json.load(local_state_file)
            info_cache = local_state.get("profile", {}).get("info_cache", {})
            for directory, details in info_cache.items():
                name = str(details.get("name", "")).strip()
                if name.lower() == profile_name.lower():
                    logging.info(
                        f"Resolved Edge profile name '{profile_name}' to directory '{directory}'."
                    )
                    return directory
        except FileNotFoundError:
            logging.warning(
                "Edge Local State file not found; falling back to profile_directory."
            )
        except (json.JSONDecodeError, OSError) as exc:
            logging.warning(
                f"Could not parse Edge Local State file: {exc}. Falling back to profile_directory."
            )

    if profile_directory:
        return profile_directory

    return default_directory

if __name__ == "__main__":
    import argparse
    import sys
    config = configparser.ConfigParser()
    config.read("config.ini")
    looker_url = config["looker"]["report_url"]
    windows_username = config.get("windows", "username", fallback=None)
    if not windows_username:
        logging.error("No Windows username found in config.ini under [windows] section.")
        sys.exit(1)
    edge_user_data_dir = fr"C:\\Users\\{windows_username}\\AppData\\Local\\Microsoft\\Edge\\User Data"
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
        from selenium.webdriver.edge.options import Options
        from selenium.webdriver.edge.service import Service
        edge_options = Options()
        edge_options.add_argument(fr"--user-data-dir={edge_user_data_dir}")
        profile_directory = resolve_edge_profile_directory(edge_user_data_dir, config)
        edge_options.add_argument(f"--profile-directory={profile_directory}")
        logging.info(f"Using Edge profile directory: {profile_directory}")
        # Redirect browser stderr to suppress GPU/Chromium errors
        edge_service = Service(stderr=open(os.devnull, 'w'))
        driver = webdriver.Edge(options=edge_options, service=edge_service)
        driver.get(looker_url)
        logging.info(f"Opened URL: {looker_url}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "date-text"))
        )

        if select_looker_date_range(driver, start_day):
            logging.info("Date selection completed successfully.")
            if export_data_to_google_sheets(driver):
                logging.info("Export to Google Sheets completed successfully.")
                # Switch to the tab with the correct sheet title and share it
                if switch_to_sheet_tab_by_title(driver, sheet_title="PumpFuse_new"):
                    logging.info("Switched to Google Sheet tab for sharing.")
                    if wait_for_google_sheet_ready(driver, timeout=60):
                        share_google_sheet_with_service_account(driver)
                    else:
                        logging.error("Google Sheet did not load in time for sharing.")
                else:
                    logging.warning("Google Sheet tab not found for sharing.")
            else:
                logging.error("Export to Google Sheets failed.")
        else:
            logging.error("Date selection failed.")

    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if driver:
            logging.info("Leaving Looker Studio page open for user inspection. Close the browser window manually when done.")
            input("Press Enter to close the browser and exit the script...\n")
            logging.info("User requested shutdown. Closing browser.")
            try:
                # Suppress urllib3 and selenium warnings during shutdown
                import logging as pylogging
                for noisy_logger in [
                    'urllib3.connectionpool',
                    'urllib3.util.retry',
                    'selenium.webdriver.remote.remote_connection',
                    'selenium.webdriver.remote.errorhandler',
                ]:
                    pylogging.getLogger(noisy_logger).setLevel(pylogging.ERROR)
                driver.quit()
            except Exception as e:
                import traceback
                # Suppress expected connection errors on shutdown (e.g., ConnectionResetError, urllib3 warnings)
                err_str = str(e)
                if any(msg in err_str for msg in [
                    'ConnectionResetError',
                    'Failed to establish a new connection',
                    'actively refused',
                    'connection was forcibly closed',
                    'invalid session id',
                    'invalid after WaitForGetOffsetInRange',
                    'Retry(total=',
                    'NewConnectionError',
                    'MaxRetryError',
                    'HTTPConnection object',
                ]):
                    logging.debug(f"Suppressed expected shutdown error: {e}")
                    logging.debug(traceback.format_exc())
                else:
                    logging.error(f"Unexpected error during driver.quit(): {e}")
                    logging.debug(traceback.format_exc())