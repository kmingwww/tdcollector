
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import logging
import os
from common import retry
from common.configurations import (
    get_default_credential_file,
    get_default_spreadsheet_id,
)

logger = logging.getLogger(__name__)

# Use read/write scope so this module can both read and update sheets.
# If you restrict to readonly, writing will fail.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _get_sheets_service(credential_file=None):
    """Internal helper to build the Google Sheets service with error handling."""
    if credential_file is None:
        credential_file = get_default_credential_file()
        
    if not os.path.exists(credential_file):
        raise FileNotFoundError(f"Credential file not found at: {credential_file}. Please check your .env or the file path.")
        
    try:
        creds = Credentials.from_service_account_file(credential_file, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False).spreadsheets()
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets service: {e}")
        raise


class GSheetManager:
    def __init__(self, sheet_range, spreadsheet_id=None, key_column="order_id", credential_file=None):
        if spreadsheet_id is None:
            spreadsheet_id = get_default_spreadsheet_id()
            
        self.spreadsheet_id = spreadsheet_id
        self.sheet_range = sheet_range
        self.key_column = key_column
        self.credential_file = credential_file

        # Maps to be populated by _refresh_metadata
        self.header_map = {}  # {col_name: index}
        self.row_map = {}     # {key_value: row_number_1_indexed}
        self.sheet_name = ""
        if "!" in sheet_range:
            self.sheet_name = sheet_range.split("!")[0]

        self.sheet = _get_sheets_service(self.credential_file)
        self._refresh_metadata()


    @property
    def sheet_prefix(self):
        return f"{self.sheet_name}!" if self.sheet_name else ""

    def _refresh_metadata(self):
        """Fetches header and key column to build maps."""
        # 1. Get headers (Row 1)
        header_result = self.sheet.values().get(
            spreadsheetId=self.spreadsheet_id, 
            range=f"{self.sheet_prefix}1:1"
        ).execute()
        headers = header_result.get("values", [[]])[0]
        self.header_map = {name: i for i, name in enumerate(headers)}
        
        # Use case-insensitive matching for the key_column
        actual_key_column = self._get_column_name_case_insensitive(self.key_column)
        if not actual_key_column:
            raise ValueError(f"Key column '{self.key_column}' not found in sheet headers")
        
        self.key_column = actual_key_column  # Update to the actual case in the sheet
            
        # 2. Get key column values
        key_col_letter = self._get_column_letter(self.header_map[self.key_column])
        key_result = self.sheet.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.sheet_prefix}{key_col_letter}:{key_col_letter}"
        ).execute()
        key_values = key_result.get("values", [])
        
        # 3. Build row map (skipping header at index 0)
        # Assuming key_values[0] is the header
        self.row_map = {str(val[0]): i + 2 for i, val in enumerate(key_values[1:]) if val}

    def _get_column_letter(self, index):
        letter = ""
        while index >= 0:
            letter = chr(index % 26 + 65) + letter
            index = index // 26 - 1
        return letter

    def _get_column_name_case_insensitive(self, target_name):
        """Finds the actual column name in the sheet that matches the target name (case-insensitive)."""
        target_lower = target_name.lower()
        for actual_name in self.header_map.keys():
            if actual_name.lower() == target_lower:
                return actual_name
        return None

    @retry(exceptions=TimeoutError, tries=5, delay=10)
    def read(self):
        """Reads the entire sheet range and returns a DataFrame."""
        result = self.sheet.values().get(
            spreadsheetId=self.spreadsheet_id, 
            range=self.sheet_range
        ).execute()
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

    def _build_update_requests(self, row_idx, df_row):
        """
        Builds surgical update requests by grouping adjacent columns into 
        contiguous ranges to minimize payload size and request count.
        """
        requests = []
        
        # 1. Get all columns we want to update that exist in the sheet
        valid_cols = []
        for col_name, value in df_row.items():
            if col_name in self.header_map and col_name != self.key_column:
                valid_cols.append((self.header_map[col_name], value))
        
        # 2. Sort columns by their index to find neighbors
        valid_cols.sort(key=lambda x: x[0])
        
        if not valid_cols:
            return requests

        # 3. Group contiguous columns
        # Example: if we have col indices 2, 3, 5, 6, 7
        # We want groups: [[2, 3], [5, 6, 7]]
        groups = []
        if valid_cols:
            current_group = [valid_cols[0]]
            for i in range(1, len(valid_cols)):
                # If this column is immediately after the previous one
                if valid_cols[i][0] == valid_cols[i-1][0] + 1:
                    current_group.append(valid_cols[i])
                else:
                    groups.append(current_group)
                    current_group = [valid_cols[i]]
            groups.append(current_group)

        # 4. Create one ValueRange per group
        for group in groups:
            start_col_idx = group[0][0]
            end_col_idx = group[-1][0]
            
            start_col_letter = self._get_column_letter(start_col_idx)
            end_col_letter = self._get_column_letter(end_col_idx)
            
            # Format: Sheet1!B5:D5
            if start_col_idx == end_col_idx:
                cell_range = f"{self.sheet_prefix}{start_col_letter}{row_idx}"
            else:
                cell_range = f"{self.sheet_prefix}{start_col_letter}{row_idx}:{end_col_letter}{row_idx}"
            
            # Extract values in order
            values = [[str(item[1]) if pd.notnull(item[1]) else "" for item in group]]
            
            requests.append({
                "range": cell_range,
                "values": values
            })
            
        return requests

    def _format_for_append(self, df_row):
        formatted = [""] * len(self.header_map)
        for col_name, value in df_row.items():
            if col_name in self.header_map:
                formatted[self.header_map[col_name]] = str(value) if pd.notnull(value) else ""
        return formatted

    @retry(exceptions=(TimeoutError, ConnectionError, OSError), tries=5, delay=10)
    def upsert(self, df: pd.DataFrame):
        if df.empty:
            return
        
        # Refresh metadata at the start to fetch any successfully appended rows
        # from a previous failed attempt, ensuring idempotency on retry.
        self._refresh_metadata()
        
        update_data = []
        append_data = []
        
        # If key_column is index, move it back to columns
        working_df = df.reset_index() if self.key_column in df.index.names else df.copy()
        
        for _, row in working_df.iterrows():
            key_val = str(row[self.key_column])
            if key_val in self.row_map:
                row_idx = self.row_map[key_val]
                update_data.extend(self._build_update_requests(row_idx, row))
            else:
                append_data.append(self._format_for_append(row))
        
        # 1. Execute Batch Updates in chunks to avoid payload limits
        if update_data:
            # We chunk by the number of individual cell updates. 
            # Google recommends keeping payloads under 2MB. 
            # 5,000 cells is a safe default for typical string sizes.
            MAX_BATCH_SIZE = int(os.getenv("GSHEET_BATCH_SIZE", "5000"))
            
            for i in range(0, len(update_data), MAX_BATCH_SIZE):
                chunk = update_data[i:i + MAX_BATCH_SIZE]
                body = {"data": chunk, "valueInputOption": "RAW"}
                logger.info(f"Sending batch update chunk {i//MAX_BATCH_SIZE + 1} ({len(chunk)} cells)...")
                self.sheet.values().batchUpdate(
                    spreadsheetId=self.spreadsheet_id, body=body
                ).execute()
            
        # 2. Execute Appends in chunks
        if append_data:
            # Append has a different structure; we chunk by rows.
            # Assuming 26 columns, 2000 rows is ~52,000 cells.
            MAX_APPEND_ROWS = int(os.getenv("GSHEET_APPEND_ROWS", "500"))
            
            for i in range(0, len(append_data), MAX_APPEND_ROWS):
                chunk = append_data[i:i + MAX_APPEND_ROWS]
                body = {"values": chunk}
                logger.info(f"Sending append chunk {i//MAX_APPEND_ROWS + 1} ({len(chunk)} rows)...")
                self.sheet.values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.sheet_prefix}A1",
                    valueInputOption="RAW",
                    body=body
                ).execute()
        
        # Refresh metadata to include new rows
        self._refresh_metadata()

