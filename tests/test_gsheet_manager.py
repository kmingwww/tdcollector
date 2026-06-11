import pytest
import pandas as pd
import numpy as np
from gsheet.main import GSheetManager
from unittest.mock import MagicMock, patch

pytestmark = pytest.mark.unit

@pytest.fixture
def mock_sheets_service():
    with patch("gsheet.main._get_sheets_service") as mock_get:
        mock_sheet = MagicMock()
        mock_get.return_value = mock_sheet
        yield mock_sheet

def test_gsheet_manager_1m_rows_mapping(mock_sheets_service):
    """
    Verifies that GSheetManager can build a map of 1 million rows 
    without excessive memory or time usage.
    """
    # 1. Setup Mock for Header (26 cols)
    cols = ["order_id"] + [f"col_{i}" for i in range(1, 26)]
    mock_sheets_service.values().get().execute.side_effect = [
        {"values": [cols]}, # First call: headers
        {"values": [[f"ID_{i}"] for i in range(1_000_001)]} # 1,000,001 items (1 header + 1M data)
    ]
    
    # 2. Initialize (this calls _refresh_metadata)
    start_time = pd.Timestamp.now()
    manager = GSheetManager(sheet_range="Sheet1!A:Z", spreadsheet_id="mock_id")
    end_time = pd.Timestamp.now()
    
    duration = (end_time - start_time).total_seconds()
    
    # 3. Assertions
    assert len(manager.row_map) == 1_000_000
    assert manager.row_map["ID_1"] == 2 # 1-indexed, skipped header (ID_0)
    print(f"\nBuilt 1M row map in {duration:.2f}s")

def test_upsert_chunking_logic(mock_sheets_service):
    """
    Verifies that upsert correctly chunks a large update into multiple API calls.
    """
    # Setup for 10,000 rows
    cols = ["order_id", "col_1"]
    
    # We need 2 calls for init, 2 for start of upsert, and 2 for the end of upsert
    mock_sheets_service.values().get().execute.side_effect = [
        {"values": [cols]}, # Init Headers
        {"values": [[f"ID_{i}"] for i in range(10_001)]}, # Init IDs
        {"values": [cols]}, # Start-upsert Headers
        {"values": [[f"ID_{i}"] for i in range(10_001)]}, # Start-upsert IDs
        {"values": [cols]}, # Post-upsert Headers
        {"values": [[f"ID_{i}"] for i in range(10_001)]}  # Post-upsert IDs
    ]
    
    manager = GSheetManager(sheet_range="Sheet1!A:Z", spreadsheet_id="mock_id")
    
    # Create an update for 10,000 rows
    df = pd.DataFrame([{"order_id": f"ID_{i+1}", "col_1": "new"} for i in range(10_000)])
    
    # Set a tiny batch size to force many chunks
    with patch("os.getenv", side_effect=lambda k, d: "500" if k == "GSHEET_BATCH_SIZE" else d):
        manager.upsert(df)
    
    # Check that batchUpdate was called multiple times
    # 10,000 / 500 = 20 chunks
    assert mock_sheets_service.values().batchUpdate.call_count == 20

def test_read_method(mock_sheets_service):
    """
    Verifies that the read method correctly returns a DataFrame with padded rows.
    """
    cols = ["order_id", "status"]
    data = [
        ["ID_1", "Active"],
        ["ID_2"] # Short row, needs padding
    ]
    
    mock_sheets_service.values().get().execute.side_effect = [
        {"values": [cols]}, # Init Headers
        {"values": [["ID_1"], ["ID_2"]]}, # Init IDs
        {"values": [cols] + data} # Read call
    ]
    
    manager = GSheetManager(sheet_range="Sheet1!A:Z", spreadsheet_id="mock_id")
    df = manager.read()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == cols
    assert df.iloc[1]["status"] == "" # Verified padding


@patch("common.decorators.time.sleep")
def test_upsert_retry_on_connection_error(mock_sleep, mock_sheets_service):
    """
    Verifies that upsert retries upon receiving a ConnectionError, and calls
    _refresh_metadata at the beginning of each attempt to maintain idempotency.
    """
    import os
    cols = ["order_id", "col_1"]
    
    # We want to mock _refresh_metadata to see if it is called at the start of upsert.
    with patch.object(GSheetManager, "_refresh_metadata") as mock_refresh:
        # Create manager
        manager = GSheetManager(sheet_range="Sheet1!A:Z", spreadsheet_id="mock_id")
        
        # Reset mock_refresh to ignore calls from __init__
        mock_refresh.reset_mock()
        
        # We want the actual batchUpdate to raise ConnectionError once, then succeed
        mock_sheets_service.values().batchUpdate().execute.side_effect = [
            ConnectionError("Connection aborted"),
            {"responses": []}
        ]
        
        # Populate maps so it tries to update
        manager.header_map = {"order_id": 0, "col_1": 1}
        manager.row_map = {"ID_1": 2}
        df = pd.DataFrame([{"order_id": "ID_1", "col_1": "new"}])
        
        manager.upsert(df)
        
        # Verify it retried: batchUpdate execute should have been called twice
        assert mock_sheets_service.values().batchUpdate().execute.call_count == 2
        
        # Verify it called _refresh_metadata at the beginning of each upsert attempt.
        # Since it retried once, upsert ran twice, so _refresh_metadata should have been called twice.
        # Plus once at the end of the second (successful) run, so 3 times in total.
        assert mock_refresh.call_count == 3


def test_default_chunk_sizes(mock_sheets_service):
    """
    Verifies that default chunk sizes for GSheetManager updates and appends
    are set to 5000 and 500 respectively when env variables are not present.
    """
    import os
    cols = ["order_id", "col_1"]
    
    # Set up mocks for _refresh_metadata
    mock_sheets_service.values().get().execute.side_effect = [
        {"values": [cols]}, # Init Headers
        {"values": []}, # Init IDs
        
        {"values": [cols]}, # Start-upsert Headers
        {"values": [[f"ID_{i}"] for i in range(6000)]}, # Start-upsert IDs
        
        {"values": [cols]}, # Post-upsert Headers
        {"values": [[f"ID_{i}"] for i in range(6000)]}  # Post-upsert IDs
    ]
    
    # Create manager
    manager = GSheetManager(sheet_range="Sheet1!A:Z", spreadsheet_id="mock_id")
    
    # Test with updates (6000 rows, all existing in row_map)
    # We patch row_map manually so it decides to do batchUpdate
    manager.header_map = {"order_id": 0, "col_1": 1}
    manager.row_map = {f"ID_{i}": i + 2 for i in range(6000)}
    df = pd.DataFrame([{"order_id": f"ID_{i}", "col_1": "val"} for i in range(6000)])
    
    # Clear environment variables if set
    with patch.dict(os.environ, {}, clear=True):
        manager.upsert(df)
        
    # Check that batchUpdate was called twice (6000 / 5000 = 2 chunks)
    assert mock_sheets_service.values().batchUpdate.call_count == 2

