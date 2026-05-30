import calendar
import logging
import logging.config
from datetime import datetime, timedelta
from enum import Enum
from time import sleep

import pandas as pd
import typer
from dotenv import load_dotenv
from typing_extensions import Annotated

from common.configurations import (
    logging_config,
    get_orders_sheet_range,
    get_case_ids_sheet_range
)
from gsheet.main import GSheetManager
from tm.api import get_all_order_list, get_all_staff
from tm.utils import process_order

load_dotenv()

logging.config.dictConfig(config=logging_config)
logger = logging.getLogger(__name__)

app = typer.Typer()

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
    manager = GSheetManager(sheet_range=get_orders_sheet_range())
    new_row_data = {
        "order_id": "2506000073552841",
        "customer_name": "NEW CUSTOMER",
        "status": "NEW STATUS",
        "updated_date": "2025-12-20 00:00:00",
    }
    new_row_df = pd.DataFrame([new_row_data])
    manager.upsert(new_row_df)
    
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
    created_date_from = target.strftime("%Y%m") + "01000000"
    created_date_to = target.strftime("%Y%m") + str(month_range[1]) + "235959"

    logger.info(f"Downloading data for {source.value} from {created_date_from} to {created_date_to}")
    staffs = get_all_staff()
    on_way_flag = "Y" if source == source.ongoing else "N"

    if gsheet:
        manager = GSheetManager(sheet_range=get_orders_sheet_range())
        all_new_data = []
        for idx, staff in enumerate(staffs):
            all_order = get_all_order_list(
                staff["staffId"], on_way_flag, created_date_from, created_date_to
            )
            logger.info(f"Progress {idx+1}/{len(staffs)}: [{staff["staffId"]}] {staff["staffName"]} with order {len(all_order)} counts")
            for order in all_order:
                new_row_data = process_order(staff=staff, order=order)
                all_new_data.append(new_row_data)
        
        if all_new_data:
            df_to_upsert = pd.DataFrame(all_new_data)
            # Deduplicate by order_id, keeping the newest one if there are duplicates in the fetched data
            if "updated_date" in df_to_upsert.columns:
                df_to_upsert["updated_date"] = pd.to_datetime(df_to_upsert["updated_date"])
                df_to_upsert = df_to_upsert.sort_values("updated_date").drop_duplicates("order_id", keep="last")
            else:
                df_to_upsert = df_to_upsert.drop_duplicates("order_id", keep="last")
            
            manager.upsert(df_to_upsert)
    else:
        data = []
        for idx, staff in enumerate(staffs):
            all_order = get_all_order_list(
                staff["staffId"], on_way_flag, created_date_from, created_date_to
            )
            logger.info(f"Progress {idx+1}/{len(staffs)}: [{staff["staffId"]}] {staff["staffName"]} with order {len(all_order)} counts")
            for order in all_order:
                new_row_data = process_order(staff=staff, order=order)
                data.append(new_row_data)
        if len(data) == 0:
            raise Exception("No data to copy.")
        logger.info(f"Total Orders: {len(data)}")
        df = pd.DataFrame(data)
        df = df.set_index("order_id")
        df.to_excel(f"{source.value}-{yearmonth}.xlsx")


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
def run_all():
    """Run download-data for the past 3 months (including current month) for both historical and ongoing sources with gsheet."""
    logger.info("Starting run_all process...")
    
    # Generate months newest → oldest (including current month)
    months = []
    for i in range(3):
        months.append(datetime.now() - timedelta(days=30*i))
    
    try:
        for date in months:
            year_month = date.strftime("%Y%m")
            
            # Run HISTORICAL
            logger.info(f"Running HISTORICAL for {year_month}...")
            download_data(source=Source.historical, yearmonth=year_month, gsheet=True)
            
            # Run ONGOING
            logger.info(f"Running ONGOING for {year_month}...")
            download_data(source=Source.ongoing, yearmonth=year_month, gsheet=True)
        
        logger.info("All months completed successfully.")
    except Exception as ex:
        logger.error(f"run_all failed: {ex}", exc_info=True)
        raise


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
