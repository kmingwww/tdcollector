
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import re
import logging

logger = logging.getLogger(__name__)

# Use read/write scope so this module can both read and update sheets.
# If you restrict to readonly, writing will fail.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1G-wMt-gB36Kh8yLAJS3MCKvdIhhgT73MnBNctdKbAQY"
SAMPLE_RANGE_NAME = "Sheet1!A:Z"

CREDENTIAL_FILE = "service_account_credential.json"

def read():
    creds = Credentials.from_service_account_file(CREDENTIAL_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()
    result = (sheet.values()
            .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
            .execute())
    values = result.get("values", [])

    if not values:
        logger.warning("No data found.")
        return pd.DataFrame()
    
    header = values[0]
    data = values[1:]
    
    # Pad rows that are shorter than the header
    num_columns = len(header)
    for row in data:
        row.extend([''] * (num_columns - len(row)))
  
    return pd.DataFrame(data, columns=header)


def write(df: pd.DataFrame,
          include_index = True,
          spreadsheet_id: str = SAMPLE_SPREADSHEET_ID,
          range_name: str = SAMPLE_RANGE_NAME,
          clear_before_write: bool = False,
          chunk_size: int = 10000):
    """Write a pandas DataFrame to a Google Sheet range using Sheets API.

    - `range_name` should be an A1-style range (e.g. 'Sheet2!A1', 'Sheet1!A:Z').
    - If your DataFrame has a header row, it will be written as the first row.
    - `value_input_option` can be 'RAW' or 'USER_ENTERED'.
    - Set `clear_before_write=True` to clear the range before updating (useful
      when new data is smaller than existing data in the sheet).
    - `chunk_size` is the number of rows to write in each batch.
    """
    if df is None or df.empty:
        raise ValueError("DataFrame is empty; nothing to write")
    creds = Credentials.from_service_account_file(CREDENTIAL_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()

    # Prepare values: header + rows. Handle NaN -> empty string.
    df_to_write = df.reset_index() if include_index else df.copy()
    df_to_write = df_to_write.where(pd.notnull(df_to_write), "")
    values = [list(map(str, df_to_write.columns.tolist()))] + df_to_write.values.tolist()

    if clear_before_write:
        sheet.values().clear(spreadsheetId=spreadsheet_id, range=range_name).execute()

    # Parse the sheet and starting cell from range_name
    sheet_name_part = ""
    range_spec = range_name
    if '!' in range_name:
        sheet_name_part, range_spec = range_name.split('!', 1)

    start_cell_spec = range_spec.split(':')[0]
    
    match = re.match(r"([A-Z]+)([0-9]*)", start_cell_spec)
    if not match:
        raise ValueError(f"Invalid range_name for starting cell: {range_name}")
    
    start_col = match.group(1)
    start_row_str = match.group(2)
    start_row = int(start_row_str) if start_row_str else 1

    # Write data in chunks
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        current_row = start_row + i
        
        # Calculate the range for the current chunk
        end_col_idx = len(chunk[0]) - 1
        end_col = ''
        while end_col_idx >= 0:
            end_col = chr(end_col_idx % 26 + 65) + end_col
            end_col_idx = end_col_idx // 26 - 1

        sheet_prefix = f"{sheet_name_part}!" if sheet_name_part else ""
        chunk_range = f"{sheet_prefix}{start_col}{current_row}:{end_col}{current_row + len(chunk) - 1}"

        body = {"values": chunk}
        result = sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=chunk_range,
            valueInputOption="RAW",
            body=body,
        ).execute()
    
    return result
