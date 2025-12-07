from datetime import datetime
import logging
import logging.config
import calendar
from time import sleep
import pandas as pd
import typer
from tm.api import get_all_order_list, get_all_staff, get_order_detail
from configurations import logging_config

logging.config.dictConfig(config=logging_config)
logger = logging.getLogger(__name__)

app = typer.Typer()


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
            print(
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
        print(staff)
        print(order_id)
        raise e

@app.command()
def testing():
    while True:
        try:
            staffs = get_all_staff()
            logger.info(f'Total staffs detected: {len(staffs)}')
        except Exception as ex:
            logger.warning(ex, exc_info=True)
            exit(1)
        finally:
            sleep(30)

@app.command()
def ongoing():
    print("-------------RESULT----------------")
    data = []
    staffs = get_all_staff()
    # staffs = filter(lambda x: x["staffId"] in [621433, 621414, 621576], staffs)
    for idx, staff in enumerate(staffs):
        all_order = get_all_order_list(staff["staffId"], "Y")

        print(idx, staff["staffId"], staff["staffName"], len(all_order))
        for order in all_order:
            datapoint = process_order(staff=staff, order=order)
            data.append(datapoint)

    df = pd.DataFrame(data)
    df = df.set_index("order_id")
    df.to_excel("ongoing.xlsx")
    print(df)

@app.command()
def historical(year: int, month: int):
    print("-------------RESULT----------------")
    if year < 2025 or year > 2100:
        raise Exception("Year must be in between 2025 and 2100")
    if month < 1 or month > 12:
        raise Exception("Month must be in between 1 and 12")
    target = datetime(year, month, 1)
    month_range = calendar.monthrange(year, month)
    createdDateFrom = target.strftime("%Y%m") + "01000000"
    # createdDateTo = datetime.today().strftime("%Y%m%d%H%M%S")
    createdDateTo = target.strftime("%Y%m") + str(month_range[1]) + "235959"
    print(createdDateFrom, createdDateTo)
    data = []
    staffs = get_all_staff()
    # staffs = filter(lambda x: x["staffId"] in [621394], staffs)
    for idx, staff in enumerate(staffs):
        # if staff["staffId"] != 634795:
        #     continue
        
        all_order = get_all_order_list(
            staff["staffId"], "N", createdDateFrom, createdDateTo
        )

        print(idx, staff["staffId"], staff["staffName"], len(all_order))

        for order in all_order:
            datapoint = process_order(staff=staff, order=order)
            data.append(datapoint)

    df = pd.DataFrame(data)
    df = df.set_index("order_id")
    df.to_excel("historical.xlsx")
    print(df)

if __name__ == "__main__":
    app()
