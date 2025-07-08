# PumpFuse Import and Clean Data

These programs import, clean and interpolate missing timestamp data in a Google Sheet exported from the PumpFuse logger.
PumpFuse data are exported as a Google Sheet from the PumpFuse Looker Studio web page. Weather data is obtained from open-meteo

## Programs
- getdate.py: gets the lastest date from the target sheet.
- getlooker.py: gets new data from Looker Studio and saves it to the input Google Sheet.
- import.py: imports data from the input sheet (PumpFuse_new) to the target sheet (sump_pump_run_times).
- clean.py: Sometimes PumpFuse fails to record a run event. Clean will insert rows with a time that will yield a duration that will equal the average duration of preceeding rows.
- getweather.py: Gets weather data from open-meteo and adds it to the target sheet. Note open-meteo only returns weather data through the previous day.

## Features
- Uses Selenium to scrape Looker Studio into the input Google Sheet
- Connects directly to Google Sheets using the Google Sheets API
- Detects and interpolates missing timestamps based on delta analysis
- Marks cleaned rows for traceability

## Setup Instructions

### 1. Create a Virtual Environment (Recommended)
```
python -m venv .venv
.venv\Scripts\activate  # On Windows
source .venv/bin/activate  # On Linux
```

### 2. Install Required Libraries
```
python -m pip install -r requirements.txt
```

### 3. Initial Google API Authentication
- Instructions assmue you have already setup Google Cloud and have a Google API Service Accouont.
- Go to the [Google Cloud Console](https://console.cloud.google.com/)
- Create a new project (or use an existing one)
- Enable the Google Sheets API and Google Drive API
- Create OAuth2 credentials (Service Account or OAuth Client ID)
- Download the credentials JSON file
- Place the credentials file path in `config.ini` (see below)

### 4. Configure `config.ini`

Example `config.ini`:
```
[google]
credentials_json = C:/path/to/your/credentials.json
SERVICE_ACCOUNT_USER_EMAIL = service-account@your-project.iam.gserviceaccount.com
target_sheet_name = sump_pump_run_times
input_sheet_name = PumpFuse_new

[weather]
latitude = 00.0000
longitude = 00.0000

[looker]
# Looker Studio report URL
report_url = https://lookerstudio.google.com/your-looker-report-url

[windows]
username = your_windows_username
```

### 5. Get and Prepare the Data
- activate the virtual environment: .\.venv\Scripts\activate
- Run getlooker.py to get the data from Looker Studio to Google Sheets
- Run .\import.py
- import.py will print the latest date and Sheet row number to the terminal
- Open the target spreadsheet
- Deterimine the row number to start cleaning at (suggest one row before the number printed by import.py)
- Run .\clean.py '<start_row_number>'
- Run .\getweather.py
- Delete the PumpFuse_new Google Sheet.

### 5b. Legacy Instructions for Getting and Preparing the Data without using getlooker.py
- activate the virtual environment: .\.venv\Scripts\activate
- Run Python .\getdate.py to get the latest date from the target spreadsheet
- Go to the Looker Studio View
- Change the start date to the day of the latest date in the target spreadsheet
- Right click on the data and choose Export/Google Sheets
- Remove any prior sheet named PumpFuse_new from Google Sheets
- Open the saved sheet
- Change the name to PumpFuse_new
- Share the sheet with your Google API service account email address
- Run .\import.py
- import.py will print the latest date and Sheet row number to the terminal
- Open the target spreadsheet
- Deterimine the row number to start cleaning at (suggest one row before the number printed by import.py)
- Run .\clean.py '<start_row_number>'
- Run .\getweather.py
- Delete the PumpFuse_new Google Sheet.

### 5c. Legacy Instructions for Manually Getting and Preparing the Data
- Check sump_pump_run_times for the last date entered
- Go to the Looker Studio View
- Change the start date to the day of the last date entered
- Right click on the data and choose Export/Google Sheets
- Open the saved sheet
- Insert a column to the right of Column A (Time)
- Open the previously saved sheet (will be named PumpFuse ####XXYY..._Untitled_Page_Table)
- Select cell B2 then copy the formula from the forumla bar
- Go to the newly saved sheet
- Select B2 and paste the forumala into the formula bar
- Press enter and accept the suggested auto fill
- Select column B from the time after the last timestamp in sump_pump_run_times to the end.
- Note the number Count in th elower left corner
- Paste at least that many rows after last row of data in sump_pump_run_times
- Copy the data that you previously selected in the newly saved sheet
- Paste the data into sump_pump_run_times
- Copy the delta formula in sump_pump_run_times
- Delete the old PumpFuse ####XXYY..._Untitled_Page_Table sheet

## Notes
- Cleaned rows will be marked in column D with the word `cleaned`.

## License
MIT
