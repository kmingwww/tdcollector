import os
import requests
import time
from functools import wraps
import json
from hashlib import sha256
from urllib.parse import urlencode
import re
import logging
logger = logging.getLogger(__name__)

COOKIE = os.getenv("COOKIE") or "sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22600381%22%2C%22first_id%22%3A%221962abf69b0155e-0dcd8098d3d2c5-7e433c49-3686400-1962abf69b12517%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk2MmFiZjY5YjAxNTVlLTBkY2Q4MDk4ZDNkMmM1LTdlNDMzYzQ5LTM2ODY0MDAtMTk2MmFiZjY5YjEyNTE3IiwiJGlkZW50aXR5X2xvZ2luX2lkIjoiNjAwMzgxIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22600381%22%7D%2C%22%24device_id%22%3A%221962f61ba232e8-0f7a6a9d2e908a-7e433c49-3686400-1962f61ba2422b6%22%7D; _gcl_au=1.1.1811027742.1748667053; _cs_c=1; optimize_uuid=70e467467932e3a7732860604a1dd2aeb9bd8e4614526edd29; _fbp=fb.2.1748667055632.811748730759758644; _tt_enable_cookie=1; _ttp=01JWJAVFP3XC5D5F53SXWWJ926_.tt.2; COOKIE_SHARING=%7B%22actualValue%22%3Atrue%2C%22MOE_DATA_TYPE%22%3A%22boolean%22%7D; IDENTITY_MAP=%7B%22userIdentities%22%3A%7B%22uid%22%3A%22970415-10-5849%22%7D%2C%22previousIdentities%22%3A%7B%7D%2C%22previousIdentitiesUpdatedTime%22%3A0%7D; ABTasty=uid=j0j8njz412jr7rt6&fst=1748667054832&pst=1748667054832&cst=1748698683499&ns=2&pvt=6&pvis=5&th=; INIT_PORTAL=7023; staffJobId=621576; visid_incap_3057414=7Iu357UUQvGx2tL+f2jK7u93c2gAAAAAQUIPAAAAAAA8ofTxkzZljCDb7T4CVYiB; visid_incap_2993746=IBIJXc6pRFqJ4MI+HtQAevh3c2gAAAAAQUIPAAAAAABSRD11vUAe6ZuaUbK3jQh9; _gcl_aw=GCL.1752397815.Cj0KCQjwss3DBhC3ARIsALdgYxNcjKZdE8hQev_mASueTQRqlwKjzBWa7UneXFRwKbUOnoZbvHxMje0aAgdWEALw_wcB; _gcl_gs=2.1.k1$i1752397813$u218924381; _clck=wtjqgc%7C2%7Cfxk%7C0%7C1977; _cs_id=0d1f2b24-d3f4-a7a3-d333-264eaa1a820d.1748667055.3.1752397815.1752397815.1.1782831055761.1.x; __qca=P1-86eb6c5e-f74b-402a-b12b-9fa15429770a; visid_incap_2977580=mnBc8gBHSk2/sgwjfQKwof53c2gAAAAAQUIPAAAAAABkV0g3W33g/edKadMxpHnG; visid_incap_2983153=MGn3a64KTaCp6QuAgHikclN4c2gAAAAAQUIPAAAAAACKEXoRj95Eax7TxB59ueXf; _ga=GA1.3.960195689.1748667054; ttcsid=1752397815828::GV3bgjYaulopa6CVMZOR.3.1752397912547; ttcsid_C4O96HHPGM656MIK9AEG=1752397815828::II6UIEHTVGg2EWLyOFgP.3.1752397912770; ttcsid_CFPI7NBC77U110MKPQBG=1752397815828::1V5RojB4IdZAN37qulXl.3.1752397912770; ttcsid_CHGR16JC77UC4FMFMNU0=1752397815829::KhBLDoq5VAoPespGbDXF.3.1752397912771; SESSION=%7B%22sessionKey%22%3A%229a60cb14-e1df-414a-850e-945ab437ec66%22%2C%22sessionStartTime%22%3A%222025-07-13T09%3A10%3A21.113Z%22%2C%22sessionMaxTime%22%3A1800%2C%22customIdentifiersToTrack%22%3A%5B%5D%2C%22sessionExpiryTime%22%3A1752399713768%2C%22numberOfSessions%22%3A2%7D; USER_DATA=%7B%22attributes%22%3A%5B%7B%22key%22%3A%22USER_ATTRIBUTE_USER_EMAIL%22%2C%22value%22%3A%22kelf1997%40gmail.com%22%2C%22updated_at%22%3A1752397912776%7D%2C%7B%22key%22%3A%22USER_ATTRIBUTE_USER_MOBILE%22%2C%22value%22%3A%22601133743907%22%2C%22updated_at%22%3A1752397912777%7D%2C%7B%22key%22%3A%22USER_ATTRIBUTE_UNIQUE_ID%22%2C%22value%22%3A%22970415-10-5849%22%2C%22updated_at%22%3A1748698740301%7D%5D%2C%22subscribedToOldSdk%22%3Afalse%2C%22deviceUuid%22%3A%229f52904b-d9f4-4388-b7f4-92941e1f5958%22%2C%22deviceAdded%22%3Atrue%7D; _ga_JCJKRHSLF7=GS2.1.s1752397815$o3$g1$t1752397965$j60$l0$h0; _ga_VK9SG81JEQ=GS2.1.s1752397819$o2$g1$t1752397965$j60$l0$h0; wk8XJKsjpt60BnltX9hNtw===nlCRYGb1gUGYjK0gliqA2A%3D%3D%20Pm+KN8lKAY9hn6xAuAEUyA%3D%3D; incap_ses_1671_3057414=aERkc+UQIhAyCte2ApYwFwbYmmgAAAAAsBwU55MzPkOV03BOMv8tpw==; HTML_VERSION=1754975685880; ZSMART_LOCALE=en; areaId=280; orgId=600381; userId=621576; SESSION=dbc0e56e-e035-422d-9a71-1e0890a712d6"

logger.info(COOKIE)

def rate_limit(calls_per_second):
    min_interval = 1.0 / calls_per_second
    def decorator(func):
        last_time_called = 0.0
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_time_called
            elapsed = time.perf_counter() - last_time_called
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kwargs)
            last_time_called = time.perf_counter()
            return ret
        return wrapper
    return decorator

def retry(exceptions, tries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _tries = tries
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logger.info(f"Retrying... {_tries - 1} tries left")
                    time.sleep(delay)
                    _tries -= 1
            return func(*args, **kwargs) # Last attempt
        return wrapper
    return decorator


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


def get_all_order_list(staffId, onWayFlag, createdDateFrom = None, createdDateTo = None):
    # staffId: 621394
    # onWayFlag: Y
    # createdDateFrom: 20250401000000
    # createdDateTo: 20250430235959
    page_number = 1
    results = []

    while True:
        if onWayFlag == "Y":
            data = {
                "dPartyCode": staffId,
                "pageSize": 50,
                "onWayFlag": onWayFlag,  # Y/N
                "pageNum": page_number,
                "dPartyType": "E",
                "extData": {"senario": "esales-monthly-order"},
            }
        else:
            data = {
                "dPartyCode": staffId,
                "pageSize": 50,
                "onWayFlag": onWayFlag,  # Y/N
                "pageNum": page_number,
                "dPartyType": "E",
                "extData": {"senario": "esales-monthly-order"},
                "createdDateFrom": createdDateFrom,
                "createdDateTo": createdDateTo,
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
        if("data" not in result_json):
            raise Exception(response.json())
        results = [*results, *result_json["data"]]
        if len(result_json["data"]) != data["pageSize"]:
            break
        else:
            page_number += 1
    return results
