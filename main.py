from datetime import datetime
from typing_extensions import Annotated
import logging
import logging.config
import calendar
from time import sleep
import pandas as pd
import typer
from gsheet.main import read, write
from tm.api import get_all_order_list, get_all_staff
from configurations import logging_config
from tm.utils import process_order
from enum import Enum

logging.config.dictConfig(config=logging_config)
logger = logging.getLogger(__name__)

app = typer.Typer()

@app.command()
def testing():
    staffs = get_all_staff()
    staffs = [s for s in staffs if s["staffName"].startswith("THAM LEONG")]
    print(staffs)

@app.command()
def testing_loop():
    while True:
        try:
            staffs = get_all_staff()
            logger.info(f"Total staffs detected: {len(staffs)}")
        except Exception as ex:
            logger.warning(ex, exc_info=True)
            exit(1)
        finally:
            sleep(30)

@app.command()
def testing_write():
    df = read()
    df.set_index("order_id", inplace=True)
    new_row_data = {
        "order_id": "2506000073552841",
        "customer_name": "NEW CUSTOMER",
        "status": "NEW STATUS",
        "updated_date": "2025-12-20 00:00:00",
    }
    new_order_id = new_row_data['order_id']
    new_row_df = pd.DataFrame([new_row_data]).set_index("order_id")

    if new_order_id in df.index:
        existing_updated_date = pd.to_datetime(df.loc[new_order_id, "updated_date"])
        new_updated_date = pd.to_datetime(new_row_df.loc[new_order_id, "updated_date"])

        if new_updated_date > existing_updated_date:
            df.update(new_row_df)
    else:
        df = pd.concat([df, new_row_df])
    write(df)
    
class Source(str, Enum):
    ongoing = "ongoing"
    historical = "historical"

@app.command()
def download_data(
    source: Annotated[
        Source,
        typer.Option(
            help="Choose between 'ongoing' and 'historical'.", 
            case_sensitive=False
        ),
    ] = "ongoing",
    yearmonth: Annotated[
        str, typer.Option(help="Year and month. Example: 202506")
    ] = f"{datetime.now().strftime("%Y%m")}",
    gsheet: Annotated[bool, typer.Option(help="Use gsheet.")] = False,
):
    if len(yearmonth) != 6:
        raise Exception("Year month must be in YYYYMM format")
    year = int(yearmonth[0:4])
    month = int(yearmonth[4:6])
    if year < 2025 or year > 2100:
        raise Exception("Year must be in between 2025 and 2100")
    if month < 1 or month > 12:
        raise Exception("Month must be in between 1 and 12")
    target = datetime(year, month, 1)
    month_range = calendar.monthrange(year, month)
    createdDateFrom = target.strftime("%Y%m") + "01000000"
    createdDateTo = target.strftime("%Y%m") + str(month_range[1]) + "235959"

    logger.info(f"Downloading data for {source} from {createdDateFrom} to {createdDateTo}")
    staffs = get_all_staff()
    # staffs = [s for s in staffs if s["staffName"].startswith("THAM LEONG")]

    if gsheet:
        df = read()
        df.set_index("order_id", inplace=True)
        for idx, staff in enumerate(staffs):
            all_order = get_all_order_list(
                staff["staffId"], "N", createdDateFrom, createdDateTo
            )
            logger.info(f"Progress {idx+1}/{len(staffs)}: [{staff["staffId"]}] {staff["staffName"]} with order {len(all_order)} counts")
            for order in all_order:
                new_row_data = process_order(staff=staff, order=order)
                new_order_id = new_row_data["order_id"]
                new_row_df = pd.DataFrame([new_row_data]).set_index("order_id")
                if new_order_id in df.index:
                    existing_updated_date = pd.to_datetime(
                        df.loc[new_order_id, "updated_date"]
                    )
                    new_updated_date = pd.to_datetime(
                        new_row_df.loc[new_order_id, "updated_date"]
                    )

                    if new_updated_date > existing_updated_date:
                        df.update(new_row_df)
                else:
                    df = pd.concat([df, new_row_df])
        write(df)
    else:
        data = []
        for idx, staff in enumerate(staffs):
            all_order = get_all_order_list(
                staff["staffId"], "N", createdDateFrom, createdDateTo
            )
            logger.info(f"Progress {idx+1}/{len(staffs)}: [{staff["staffId"]}] {staff["staffName"]} with order {len(all_order)} counts")
            for order in all_order:
                new_row_data = process_order(staff=staff, order=order)
                data.append(new_row_data)
        df = pd.DataFrame(data)
        df = df.set_index("order_id")
        df.to_excel(f"{yearmonth if yearmonth else 'ongoing'}.xlsx")


@app.command()
def ongoing():
    logger.info("-------------RESULT----------------")
    data = []
    staffs = get_all_staff()
    # staffs = filter(lambda x: x["staffId"] in [621433, 621414, 621576], staffs)
    for idx, staff in enumerate(staffs):
        all_order = get_all_order_list(staff["staffId"], "Y")

        logger.info(idx, staff["staffId"], staff["staffName"], len(all_order))
        for order in all_order:
            datapoint = process_order(staff=staff, order=order)
            data.append(datapoint)

    df = pd.DataFrame(data)
    df = df.set_index("order_id")
    df.to_excel("ongoing.xlsx")


@app.command()
def historical(year: int, month: int):
    logger.info("-------------RESULT----------------")
    if year < 2025 or year > 2100:
        raise Exception("Year must be in between 2025 and 2100")
    if month < 1 or month > 12:
        raise Exception("Month must be in between 1 and 12")
    target = datetime(year, month, 1)
    month_range = calendar.monthrange(year, month)
    createdDateFrom = target.strftime("%Y%m") + "01000000"
    # createdDateTo = datetime.today().strftime("%Y%m%d%H%M%S")
    createdDateTo = target.strftime("%Y%m") + str(month_range[1]) + "235959"
    logger.info(f"{createdDateFrom} -> {createdDateTo}")
    data = []
    staffs = get_all_staff()
    # staffs = filter(lambda x: x["staffId"] in [621394], staffs)
    for idx, staff in enumerate(staffs):
        # if staff["staffId"] != 634795:
        #     continue

        all_order = get_all_order_list(
            staff["staffId"], "N", createdDateFrom, createdDateTo
        )

        logger.info(f"{idx}, {staff["staffId"]}, {staff["staffName"]}, {len(all_order)}")

        for order in all_order:
            datapoint = process_order(staff=staff, order=order)
            data.append(datapoint)

    df = pd.DataFrame(data)
    df = df.set_index("order_id")
    df.to_excel("historical.xlsx")


if __name__ == "__main__":
    app()
