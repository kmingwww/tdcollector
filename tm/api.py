import os
import requests
import json
from hashlib import sha256
from urllib.parse import urlencode
import re
import logging
from dotenv import load_dotenv
from common import rate_limit, retry

load_dotenv()

COOKIE = os.getenv("COOKIE")

logger = logging.getLogger(__name__)


def generateSigncode(method: str, path: str, data: dict):
    def hash(content):
        return sha256(content.encode("utf-8")).hexdigest()

    if method.lower() == "get":
        return hash(path + urlencode(data) + "32BytesString")
    else:
        jsonString = json.dumps(data)
        jsonString = re.sub(r"[^a-zA-Z0-9]", "", jsonString)
        jsonString = re.sub(r"null", "", jsonString)
        return hash(path + jsonString + "32BytesString")


@rate_limit(calls_per_second=1)
def get_order_list(data):
    path = "/cee/order/v2/getCeeOrderList"
    signcode = generateSigncode("post", path, data)
    # data = {
    #     "dPartyCode": 621564,
    #     "pageSize": 10,
    #     "onWayFlag": "Y",
    #     "pageNum": 1,
    #     "dPartyType": "E",
    #     "extData": {"senario": "esales-monthly-order"},
    # }
    response = requests.post(
        f"https://dealer.unifi.com.my/portal/esales/api{path}",
        headers={
            "Cookie": COOKIE,
            "signcode": signcode,
        },
        json=data,
    )
    return response


def get_all_order_list(staffId, onWayFlag, createdDateFrom=None, createdDateTo=None):
    # staffId: 621394
    # onWayFlag: Y
    # createdDateFrom: 20250401000000
    # createdDateTo: 20250430235959
    page_number = 1
    results = []

    while True:
        data = {
            "createdDateFrom": createdDateFrom,
            "createdDateTo": createdDateTo,
            "dPartyType": "E",
            "extData": {"drmOrderOptimizeQuery": "Y"},
            "onWayFlag": onWayFlag, # Y/N
            "pageNum": page_number,
            "pageSize": 50,
            "partyCodes": staffId,
        }
        response = get_order_list(data)
        result_json = response.json()
        results = [*results, *result_json["data"]]
        if len(result_json["data"]) != data["pageSize"]:
            break
        else:
            page_number += 1
    return results


@retry(exceptions=(requests.exceptions.HTTPError), tries=5, delay=10)
@rate_limit(calls_per_second=1)
def get_order_detail(data):
    path = "/cee/order/v2/getCeeOrderDetail"
    signcode = generateSigncode("post", path, data)
    # data = {"custOrderId": "2502000060013514", "custOrderNbr": "2502000060013514"}
    response = requests.post(
        f"https://dealer.unifi.com.my/portal/esales/api{path}",
        headers={
            "Cookie": COOKIE,
            "signcode": signcode,
        },
        json=data,
    )
    response.raise_for_status()
    return response


@rate_limit(calls_per_second=1)
def get_staff_detail(data):
    path = "/saleschannel/getStaffDetail"
    signcode = generateSigncode("post", path, data)
    response = requests.post(
        f"https://dealer.unifi.com.my/portal/esales/api{path}",
        headers={
            "Cookie": COOKIE,
            "signcode": signcode,
        },
        json=data,
    )
    return response


@rate_limit(calls_per_second=1)
def get_staff(data):
    path = "/saleschannel/qryStaffList"
    signcode = generateSigncode("post", path, data)
    response = requests.post(
        f"https://dealer.unifi.com.my/portal/esales/api{path}",
        headers={
            "Cookie": COOKIE,
            "signcode": signcode,
        },
        json=data,
    )
    return response


def get_all_staff():
    page_number = 1
    results = []

    while True:
        data = {"pageSize": 50, "pageNum": page_number}
        response = get_staff(data)
        result_json = response.json()
        if "data" not in result_json:
            raise Exception(response.json())
        results = [*results, *result_json["data"]]
        if len(result_json["data"]) != data["pageSize"]:
            break
        else:
            page_number += 1
    return results
