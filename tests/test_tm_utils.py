import pytest
from unittest.mock import MagicMock, patch

pytestmark = pytest.mark.unit


# --- helpers ---


def _make_staff(name="TEST STAFF", code="TS123", org="TEST CHANNEL"):
    return {"staffName": name, "staffCode": code, "orgName": org}


def _make_order(order_id="12345", order_nbr="ORD123", state="COMPLETED",
                accept_date="2026-05-30", state_date="2026-05-30",
                main_offer="TEST BUNDLE", event_type="NEW"):
    return {
        "orderId": order_id,
        "orderNbr": order_nbr,
        "stateName": state,
        "acceptDate": accept_date,
        "stateDate": state_date,
        "mainOfferName": main_offer,
        "eventTypeName": event_type,
    }


def _make_order_detail_response():
    """Minimal mock get_order_detail response for process_order tests."""
    mock = MagicMock()
    mock.json.return_value = {
        "data": {
            "orderId": "12345",
            "stateName": "COMPLETED",
            "acceptDate": "2026-05-30",
            "stateDate": "2026-05-30",
            "orderItemList": [],
            "installationInfoList": [],
            "custInfo": {},
            "eventTypeName": "NEW",
        }
    }
    return mock


# --- process_order ---


def test_process_order_carries_staff_code():
    from tm.utils import process_order

    staff = _make_staff(code="ABC123")
    order = _make_order()

    with patch("tm.utils.get_order_detail") as mock_get_detail:
        mock_get_detail.return_value = _make_order_detail_response()
        datapoint = process_order(staff, order)

    assert datapoint["staff_code"] == "ABC123"


def test_process_order_carries_channel_name():
    from tm.utils import process_order

    staff = _make_staff(org="MY CHANNEL")
    order = _make_order()

    with patch("tm.utils.get_order_detail") as mock_get_detail:
        mock_get_detail.return_value = _make_order_detail_response()
        datapoint = process_order(staff, order)

    assert datapoint["channel_name"] == "MY CHANNEL"


# --- _build_fallback_datapoint ---


def test_fallback_datapoint_carries_staff_code():
    from tm.utils import _build_fallback_datapoint

    staff = _make_staff(code="FALLBACK_CODE")
    order = _make_order()
    datapoint = _build_fallback_datapoint(staff, order)

    assert datapoint["staff_code"] == "FALLBACK_CODE"


def test_fallback_datapoint_carries_channel_name():
    from tm.utils import _build_fallback_datapoint

    staff = _make_staff(org="FALLBACK_CHANNEL")
    order = _make_order()
    datapoint = _build_fallback_datapoint(staff, order)

    assert datapoint["channel_name"] == "FALLBACK_CHANNEL"


# --- get_residential_voice_number ---


def test_get_residential_voice_number_concatenates_prefix_and_accnbr():
    from tm.utils import get_residential_voice_number
    item = {"prefix": "03", "accNbr": "12345678"}
    assert get_residential_voice_number(item) == "0312345678"


def test_get_residential_voice_number_returns_none_when_prefix_missing():
    from tm.utils import get_residential_voice_number
    assert get_residential_voice_number({"accNbr": "12345678"}) is None


def test_get_residential_voice_number_returns_none_when_accnbr_missing():
    from tm.utils import get_residential_voice_number
    assert get_residential_voice_number({"prefix": "03"}) is None


def test_get_residential_voice_number_returns_none_for_empty_dict():
    from tm.utils import get_residential_voice_number
    assert get_residential_voice_number({}) is None
