# Data Collector
1. Sign in to the platform
2. Ctrl + Shift + C to open developer console
3. Go to network tab, copy `Cookie` value in any request 
4. Replace the `COOKIE` value you copied in step 3 in `tm/api.py` file
5. Run data synchronization commands:
   - **Download Orders:** `uv run main.py download-data --source ongoing --yearmonth 202506 --gsheet`
     - `--source`: can be `ongoing` or `historical`
     - `--yearmonth`: format in `YYYYMM`
     - `--gsheet`: optional, use this flag to save to Google Sheets. If omitted, output is saved to an Excel file.
   - **Sync Case Status:** `uv run main.py sync-cases`
     - Fetches latest status and Troika ID for all cases listed in the "CASE ID" Google Sheet.

# Guidelines
- https://www.dash0.com/guides/logging-in-python
- https://www.youtube.com/watch?v=9L77QExPmI0

# Testing
To run the unit tests, use the following command:
```bash
uv run pytest tests/test_gsheet_manager.py -s
```
This will run the mocked scalability tests (simulating 1 million rows and chunking logic) without requiring actual Google Sheets credentials.
