import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_target():
    """Late-import GSheetManager so import failures don't block collection."""
    from gsheet.main import GSheetManager
    return GSheetManager


def _make_manager(sheets_mock, sheet_range="Sheet1!A:Z", key_column="order_id"):
    """Create a GSheetManager with the sheets service mocked."""
    with patch("gsheet.main._get_sheets_service", return_value=sheets_mock):
        return _make_target()(
            sheet_range=sheet_range,
            spreadsheet_id="mock_id",
            key_column=key_column,
        )


def _mock_header_and_key_values(sheets_mock, headers, key_values):
    """Set up the two get().execute() calls that _refresh_metadata makes."""
    sheets_mock.values().get().execute.side_effect = [
        {"values": [headers]},
        {"values": key_values},
    ]


# ---------------------------------------------------------------------------
# _refresh_metadata  (implicitly tested via GSheetManager.__init__)
# ---------------------------------------------------------------------------


def test_row_map_is_list_based_for_unique_keys():
    """Each key should map to a list of row indices, even with a single match."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "status"],
        key_values=[["order_id"], ["ID_A"], ["ID_B"], ["ID_C"]],
    )

    manager = _make_manager(sheets)

    assert manager.row_map["ID_A"] == [2]
    assert manager.row_map["ID_B"] == [3]
    assert manager.row_map["ID_C"] == [4]
    assert all(isinstance(v, list) for v in manager.row_map.values())


def test_refresh_metadata_skips_falsy_key_values():
    """Empty-list rows are skipped; non-empty rows with '' are kept (existing behaviour)."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "status"],
        key_values=[["order_id"], ["ID_1"], [], ["ID_2"], [""]],
    )

    manager = _make_manager(sheets)

    assert len(manager.row_map) == 3
    assert "ID_1" in manager.row_map
    assert "ID_2" in manager.row_map
    assert "" in manager.row_map  # [""] is truthy, keyed as empty string


def test_duplicate_key_stores_all_row_indices():
    """When a key appears multiple times, all row indices are captured."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id"],
        key_values=[["order_id"], ["DUP"], ["B"], ["C"], ["DUP"], ["D"], ["DUP"]],
    )

    manager = _make_manager(sheets)

    assert manager.row_map["DUP"] == [2, 5, 7]


def test_duplicate_key_logs_warning(caplog):
    """A warning is emitted listing the count and first few duplicate IDs."""
    import logging
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id"],
        key_values=[["order_id"], ["DUP"], ["B"], ["DUP"], ["C"]],
    )

    with caplog.at_level(logging.WARNING):
        manager = _make_manager(sheets)

    assert "Found 1 duplicate key(s)" in caplog.text
    assert "'order_id'" in caplog.text
    assert "DUP" in caplog.text
    assert "All occurrences will be updated" in caplog.text


def test_no_warning_when_all_keys_unique(caplog):
    """No duplicate warning should appear for a clean sheet."""
    import logging
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id"],
        key_values=[["order_id"], ["A"], ["B"], ["C"]],
    )

    with caplog.at_level(logging.WARNING):
        _make_manager(sheets)

    assert "duplicate" not in caplog.text.lower()


# ---------------------------------------------------------------------------
# upsert — update path (existing keys)
# ---------------------------------------------------------------------------


def test_upsert_sends_batch_update_for_existing_key():
    """When a key exists in row_map, a batchUpdate request is fired."""
    sheets = MagicMock()
    # _refresh_metadata at init
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "col_1"],
        key_values=[["order_id"], ["ID_1"]],
    )

    manager = _make_manager(sheets)

    # Bypass the internal _refresh_metadata inside upsert so we can control
    # what gets called on the API (avoids fragile 6-entry side_effect lists).
    with patch.object(manager, "_refresh_metadata"):
        manager.header_map = {"order_id": 0, "col_1": 1}
        manager.row_map = {"ID_1": [2]}

        df = pd.DataFrame([{"order_id": "ID_1", "col_1": "new_value"}])
        manager.upsert(df)

    sheets.values().batchUpdate.assert_called_once()


def test_upsert_updates_all_duplicate_rows():
    """Every row for a duplicate key receives an update range in the request."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "status"],
        key_values=[["order_id"], ["ID_1"], ["ID_2"], ["ID_1"]],
    )
    manager = _make_manager(sheets)

    with patch.object(manager, "_refresh_metadata"):
        manager.header_map = {"order_id": 0, "status": 1}
        manager.row_map = {"ID_1": [2, 4], "ID_2": [3]}

        df = pd.DataFrame([
            {"order_id": "ID_1", "status": "Updated"},
            {"order_id": "ID_2", "status": "Also"},
        ])
        manager.upsert(df)

    body = sheets.values().batchUpdate.call_args[1]["body"]
    ranges = [d["range"] for d in body["data"]]
    assert "Sheet1!B2" in ranges  # ID_1 first occurrence
    assert "Sheet1!B4" in ranges  # ID_1 duplicate
    assert "Sheet1!B3" in ranges  # ID_2
    assert len(ranges) == 3


# ---------------------------------------------------------------------------
# upsert — append path (new keys)
# ---------------------------------------------------------------------------


def test_upsert_appends_new_key():
    """When a key is not in row_map, its row should be appended via values().append."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "col_1"],
        key_values=[["order_id"], ["ID_1"]],
    )
    manager = _make_manager(sheets)

    with patch.object(manager, "_refresh_metadata"):
        manager.header_map = {"order_id": 0, "col_1": 1}
        manager.row_map = {"ID_1": [2]}

        df = pd.DataFrame([{"order_id": "ID_NEW", "col_1": "fresh"}])
        manager.upsert(df)

    sheets.values().append.assert_called_once()


# ---------------------------------------------------------------------------
# upsert — retry
# ---------------------------------------------------------------------------


@patch("common.decorators.time.sleep")
def test_upsert_retries_on_connection_error(mock_sleep):
    """When batchUpdate raises ConnectionError, upsert should retry."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "col_1"],
        key_values=[["order_id"], ["ID_1"]],
    )
    manager = _make_manager(sheets)

    with patch.object(manager, "_refresh_metadata"):
        manager.header_map = {"order_id": 0, "col_1": 1}
        manager.row_map = {"ID_1": [2]}

        sheets.values().batchUpdate().execute.side_effect = [
            ConnectionError("aborted"),
            {"responses": []},
        ]

        df = pd.DataFrame([{"order_id": "ID_1", "col_1": "new"}])
        manager.upsert(df)

    assert sheets.values().batchUpdate().execute.call_count == 2


# ---------------------------------------------------------------------------
# upsert — chunking
# ---------------------------------------------------------------------------


def test_upsert_chunks_large_update_into_multiple_api_calls():
    """A large number of updates should be split across multiple batchUpdate calls."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id", "col_1"],
        key_values=[["order_id"], *[[f"ID_{i}"] for i in range(500)]],
    )
    manager = _make_manager(sheets)

    with patch.object(manager, "_refresh_metadata"):
        manager.header_map = {"order_id": 0, "col_1": 1}
        # 500 existing rows, chunk size forced to 100
        manager.row_map = {f"ID_{i}": [i + 2] for i in range(500)}
        df = pd.DataFrame([{"order_id": f"ID_{i}", "col_1": "val"} for i in range(500)])

        with patch("gsheet.main.os.getenv", side_effect=lambda k, d: "100" if k == "GSHEET_BATCH_SIZE" else d):
            manager.upsert(df)

    # 500 updates / 100 per chunk = 5 batchUpdate calls
    assert sheets.values().batchUpdate.call_count == 5


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


def test_read_returns_dataframe_with_padded_short_rows():
    """read() should return a DataFrame and pad rows shorter than the header."""
    sheets = MagicMock()
    headers = ["order_id", "status"]
    data = [["ID_1", "Active"], ["ID_2"]]  # second row missing status

    _mock_header_and_key_values(
        sheets,
        headers=headers,
        key_values=[["order_id"], ["ID_1"], ["ID_2"]],
    )
    manager = _make_manager(sheets)

    # Now add the read() response
    sheets.values().get().execute.side_effect = None
    sheets.values().get().execute.return_value = {"values": [headers] + data}

    df = manager.read()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == headers
    assert df.iloc[1]["status"] == ""


# ---------------------------------------------------------------------------
# performance / scale
# ---------------------------------------------------------------------------


def test_row_map_handles_one_million_rows():
    """Building row_map for 1M rows should complete in reasonable time."""
    sheets = MagicMock()
    _mock_header_and_key_values(
        sheets,
        headers=["order_id"],
        key_values=([["order_id"]]
                     + [[f"ID_{i}"] for i in range(1, 1_000_001)]),
    )

    manager = _make_manager(sheets)

    assert len(manager.row_map) == 1_000_000
    assert manager.row_map["ID_1"] == [2]
