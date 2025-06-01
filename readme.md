# Pumpfuse Clean Data

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
pip install -r requirements.txt
```

### 3. Google API Authentication
- Go to the [Google Cloud Console](https://console.cloud.google.com/)
- Create a new project (or use an existing one)
- Enable the Google Sheets API and Google Drive API
- Create OAuth2 credentials (Service Account or OAuth Client ID)
- Download the credentials JSON file
- Place the credentials file path and other required keys in `config.ini` (see below)

### 4. Configure `config.ini`
Example `config.ini`:
```
[google]
credentials_json = path/to/your/credentials.json
sheet_name = sump_pump_run_times_copy
```

### 5. Run the Program
```
python clean.py <start_row_number>
```
- Replace `<start_row_number>` with the row number to start cleaning from (e.g., 2).

## Notes
- The program will only modify a copy of your data (as specified in the sheet name).
- Cleaned rows will be marked in column D with the word `cleaned`.
- Make sure `config.ini` is in the same directory as `clean.py`.

## License
MIT
