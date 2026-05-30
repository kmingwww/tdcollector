import pytest
import os
from tm.api import get_staff, get_staff_detail, get_order_list, get_order_detail, get_case_detail

# Skip tests if COOKIE is not set in environment
pytestmark = pytest.mark.skipif(
    not os.getenv("COOKIE"),
    reason="COOKIE environment variable not set"
)

@pytest.fixture(scope="module")
def shared_data():
    """Helper to share data between tests to avoid redundant calls."""
    return {}

def test_staff_api_chain(shared_data):
    # 1. Get staff list to find a valid staffId
    data = {"pageSize": 10, "pageNum": 1}
    response = get_staff(data)
    assert response.status_code == 200
    result = response.json()
    assert "data" in result and len(result["data"]) > 0
    
    # Save for other tests
    shared_data["staff"] = result["data"][0]
    staff_id = shared_data["staff"]["staffId"]
    
    # 2. Test staff detail with dynamic ID
    detail_response = get_staff_detail({"staffId": str(staff_id)})
    assert detail_response.status_code == 200
    assert detail_response.json()["code"] == "200"

def test_order_api_chain(shared_data):
    # 1. Get staff list if not already available
    if "staff_list" not in shared_data:
        data = {"pageSize": 20, "pageNum": 1}
        response = get_staff(data)
        shared_data["staff_list"] = response.json().get("data", [])
        
    staff_list = shared_data["staff_list"]
    found_order = False
    
    # 2. Iterate through staff until we find one with orders
    for staff in staff_list:
        staff_id = staff["staffId"]
        order_data = {
            "partyCodes": str(staff_id),
            "pageSize": 10,
            "pageNum": 1,
            "onWayFlag": "Y",
            "dPartyType": "E",
        }
        response = get_order_list(order_data)
        if response.status_code == 200:
            result = response.json()
            if result.get("data"):
                shared_data["staff"] = staff
                shared_data["order"] = result["data"][0]
                found_order = True
                break
                
    if not found_order:
        pytest.skip("Could not find any staff with orders in the first 20 results")
    
    # 3. Test order detail with dynamic IDs
    order_detail_data = {
        "custOrderId": shared_data["order"]["orderId"],
        "custOrderNbr": shared_data["order"]["orderNbr"]
    }
    detail_response = get_order_detail(order_detail_data)
    assert detail_response.status_code == 200
    assert detail_response.json()["code"] == "200"

def test_get_case_detail_real():
    # Hardcoded as requested since valid IDs are hard to find dynamically
    case_id = "103902998"
    response = get_case_detail({"caseId": case_id})
    assert response.status_code == 200
    assert response.json()["code"] == "200"
