# Pumpfuse Import and Clean Data

This program cleans and interpolates missing timestamp data in a Google Sheet exported from the Pumpfuse logger.

## Features
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

### 3. Google API Authentication
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
credentials_json = path/to/your/credentials.json
target_sheet_name = sump_pump_run_times
input_sheet_name = PumpFuse_new
```

### 5. Get and Prepare the Data
- Run Python .\getdate.py to get the latest date from the target spreadsheet
- Go to the Looker Studio View
- Change the start date to the day of the last date entered
- Right click on the data and choose Export/Google Sheets
- Open the saved sheet
- Change the name to PumpFuse_new
- Share the sheet with python-sheets@jsl-python-sheets.iam.gserviceaccount.com
- Run .\import.py
- Open the target spreadsheet
- Deterimine the row number to start cleaning at
- Run .\clean.py [row number]

### 5b. Legacy Instructions for Manually Getting and Preparing the Data
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

### 6. Activate the Virtual Environment and Run the Program
```
.\.venv\Scripts\activate
python clean.py <start_row_number>
```
- Replace `<start_row_number>` with the row number to start cleaning from (e.g., 2).

## Notes
- Cleaned rows will be marked in column D with the word `cleaned`.

## License
MIT
