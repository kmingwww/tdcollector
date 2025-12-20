
from tm.api import get_order_detail
import logging

logger = logging.getLogger(__name__)

def get_residential_voice_number(residential_voice_item):
    if "prefix" in residential_voice_item and "accNbr" in residential_voice_item:
        return residential_voice_item["prefix"] + residential_voice_item["accNbr"]
    return None


def process_order(staff, order):
    order_id = order["orderId"]
    get_order_detail_data = {
        "custOrderId": order["orderId"],
        "custOrderNbr": order["orderNbr"],
    }
    get_order_detail_response = get_order_detail(get_order_detail_data)
    order_detail = get_order_detail_response.json()["data"]
    order_items = order_detail["orderItemList"]
    installation_info_list = order_detail["installationInfoList"]

    try:
        if len(installation_info_list) != 1:
            logger.info(
                f"WARNING: NO INSTALLATION POSSIBLE for {staff["staffName"]} - {order_detail.get("orderId")}"
            )

        installation_info = (
            installation_info_list[0] if len(installation_info_list) > 0 else None
        )
        # bundle_items = next((x for x in order_items if x["serviceType"] == 51), None)
        bundle_items = next((x for x in order_items if x["serviceType"] == 51), None)
        internet_items = next((x for x in order_items if x["serviceType"] == 79), None)
        residential_voice_items = next((x for x in order_items if x["serviceType"] == 80), None)
        dms_items = next((x for x in order_items if x["serviceType"] == 924), None)
        cloud_storage_item = next((x for x in order_items if x["serviceType"] == 888), None)
        uni5g_items = next((x for x in order_items if x["serviceType"] == 15), None)

        datapoint = {
            "order_id": str(order_detail.get("orderId")),
            "staffName": staff.get("staffName"),
            "status": order_detail.get("stateName"),
            "created_date": order_detail.get("acceptDate"),  # created date
            "updated_date": order_detail.get("stateDate"),  # update date
            "installation_contact_name": (
                installation_info.get("custContactDto", {}).get("contactName")
                if installation_info
                else None
            ),
            "installation_contact_email": (
                installation_info.get("custContactDto", {}).get("email")
                if installation_info
                else None
            ),
            "installation_contact_phone": (
                installation_info.get("custContactDto", {}).get("contactNbr")
                if installation_info
                else None
            ),
            "installation_start_time": (
                installation_info.get("appointmentInfo", {}).get("appointmentStartTime")
                if installation_info
                else None
            ),
            "installation_end_time": (
                installation_info.get("appointmentInfo", {}).get("appointmentEndTime")
                if installation_info
                else None
            ),
            "installation_address": (
                installation_info.get("displayAddress") if installation_info else None
            ),
            "customer_name": order_detail.get("custInfo", {}).get("custName"),
            "customer_id_type": order_detail.get("custInfo", {}).get("certTypeName"),
            "customer_id": order_detail.get("custInfo", {}).get("certNbr"),
            "bundle_name": (
                bundle_items.get("mainOfferName") if bundle_items else None
            ),
            "tm_account_id": (
                internet_items.get("accNbr") if internet_items else None
            ),
            "account_nbr": (
                internet_items.get("acctNbr") if internet_items else None
            ),
            "residential_number": (
                get_residential_voice_number(residential_voice_items)
                if residential_voice_items
                else None
            ),
            "event_type_name": order_detail.get("eventTypeName"),
            "dms_item": (
                dms_items.get("feeList")[0].get("priceName")
                if dms_items
                else None
            ),
            "cloud_storage_item": (
                next((x for x in cloud_storage_item.get("offerInstList") if x["offerType"] == "4"), {}).get("offerName")
                # cloud_storage_item.get("offerInstList")[0].get("offerName")
                if cloud_storage_item and cloud_storage_item.get("offerInstList")
                else None
            ),
            "uni5g_items": uni5g_items.get("mainOfferName") if uni5g_items else None
        }
        return datapoint
    except Exception as e:
        logger.info(staff)
        logger.info(order_id)
        raise e
