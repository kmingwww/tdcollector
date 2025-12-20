
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd

# Use read/write scope so this module can both read and update sheets.
# If you restrict to readonly, writing will fail.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1G-wMt-gB36Kh8yLAJS3MCKvdIhhgT73MnBNctdKbAQY"
SAMPLE_RANGE_NAME = "Sheet1!A:Z"

creds = Credentials.from_service_account_file("tdcollector-cdc227d8c4e8.json", scopes=SCOPES)
service = build("sheets", "v4", credentials=creds, cache_discovery=False)

def read():
    sheet = service.spreadsheets()
    result = (sheet.values()
            .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
            .execute())
    values = result.get("values", [])

    if not values:
        print("No data found.")
        return
  
    return pd.DataFrame(values[1:], columns=values[0])


def write(df: pd.DataFrame,
          spreadsheet_id: str = SAMPLE_SPREADSHEET_ID,
          range_name: str = SAMPLE_RANGE_NAME,
          clear_before_write: bool = False):
    """Write a pandas DataFrame to a Google Sheet range using Sheets API.

    - `range_name` should be an A1-style range (e.g. 'Sheet2!A1').
    - If your DataFrame has a header row, it will be written as the first row.
    - `value_input_option` can be 'RAW' or 'USER_ENTERED'.
    - Set `clear_before_write=True` to clear the range before updating (useful
      when new data is smaller than existing data in the sheet).
    """
    if df is None or df.empty:
        raise ValueError("DataFrame is empty; nothing to write")

    sheet = service.spreadsheets()

    # Prepare values: header + rows. Handle NaN -> empty string.
    df_to_write = df.copy()

    df_to_write = df_to_write.where(pd.notnull(df_to_write), "")
    values = [list(map(str, df_to_write.columns.tolist()))] + df_to_write.values.tolist()

    if clear_before_write:
        sheet.values().clear(spreadsheetId=spreadsheet_id, range=range_name).execute()

    body = {"values": values}

    result = sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()

    return result
