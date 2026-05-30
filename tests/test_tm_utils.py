import pytest
from unittest.mock import MagicMock, patch
from tm.utils import process_order, _build_fallback_datapoint

def test_process_order_includes_staff_info():
    staff = {
        "staffName": "TEST STAFF",
        "staffCode": "TS123",
        "orgName": "TEST CHANNEL"
    }
    order = {
        "orderId": "12345",
        "orderNbr": "ORD123",
        "stateName": "COMPLETED",
        "acceptDate": "2026-05-30",
        "stateDate": "2026-05-30",
        "mainOfferName": "TEST BUNDLE",
        "eventTypeName": "NEW"
    }
    
    # Mocking get_order_detail to avoid actual API call and focus on datapoint construction
    with patch("tm.utils.get_order_detail") as mock_get_detail:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": {
                "orderId": "12345",
                "stateName": "COMPLETED",
                "acceptDate": "2026-05-30",
                "stateDate": "2026-05-30",
                "orderItemList": [],
                "installationInfoList": [],
                "custInfo": {},
                "eventTypeName": "NEW"
            }
        }
        mock_get_detail.return_value = mock_response
        
        datapoint = process_order(staff, order)
        
        assert datapoint["staff_code"] == "TS123"
        assert datapoint["channel_name"] == "TEST CHANNEL"

def test_fallback_datapoint_includes_staff_info():
    staff = {
        "staffName": "TEST STAFF",
        "staffCode": "TS123",
        "orgName": "TEST CHANNEL"
    }
    order = {
        "orderId": "12345",
        "mainOfferName": "TEST BUNDLE",
        "stateName": "COMPLETED",
        "acceptDate": "2026-05-30",
        "stateDate": "2026-05-30",
        "eventTypeName": "NEW"
    }
    
    datapoint = _build_fallback_datapoint(staff, order)
    
    assert datapoint["staff_code"] == "TS123"
    assert datapoint["channel_name"] == "TEST CHANNEL"
