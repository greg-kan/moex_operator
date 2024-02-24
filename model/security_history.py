import requests
import apimoex
import aiomoex
import aiohttp
from logger import Logger
import settings as st
import db_helper as dbh
from operator import itemgetter
from datetime import datetime
import pandas as pd
import collector.history_ex as h_ex

logger = Logger('security_history', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()

SHARES_HISTORY_BOARD = "TQBR"
SHARES_HISTORY_REQUEST_URL = (f'https://iss.moex.com/iss/history/engines/'
                              f'stock/markets/shares/boards/{SHARES_HISTORY_BOARD}/securities.json')
SHARES_HISTORY_DB_TABLE = 'history.stock_shares_securities_history'

SHARES_HISTORY_DB_TABLE_COLUMNS = ("BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES",
                                   "VALUE", "OPEN", "LOW", "HIGH", "LEGALCLOSEPRICE", "WAPRICE",
                                   "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3", "ADMITTEDQUOTE",
                                   "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL",
                                   "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR")

SHARES_HISTORY_METADATA = {
    "BOARDID": {"type": "string", "bytes": 12, "max_size": 0},
    "TRADEDATE": {"type": "date", "bytes": 10, "max_size": 0},
    "SHORTNAME": {"type": "string", "bytes": 189, "max_size": 0},
    "SECID": {"type": "string", "bytes": 36, "max_size": 0},
    "NUMTRADES": {"type": "double"},
    "VALUE": {"type": "double"},
    "OPEN": {"type": "double"},
    "LOW": {"type": "double"},
    "HIGH": {"type": "double"},
    "LEGALCLOSEPRICE": {"type": "double"},
    "WAPRICE": {"type": "double"},
    "CLOSE": {"type": "double"},
    "VOLUME": {"type": "double"},
    "MARKETPRICE2": {"type": "double"},
    "MARKETPRICE3": {"type": "double"},
    "ADMITTEDQUOTE": {"type": "double"},
    "MP2VALTRD": {"type": "double"},
    "MARKETPRICE3TRADESVALUE": {"type": "double"},
    "ADMITTEDVALUE": {"type": "double"},
    "WAVAL": {"type": "double"},
    "TRADINGSESSION": {"type": "int32"},
    "CURRENCYID": {"type": "string", "bytes": 9, "max_size": 0},
    "TRENDCLSPR": {"type": "double"}
}

BONDS_HISTORY_METADATA = {
    "BOARDID": {"type": "string", "bytes": 12, "max_size": 0},
    "TRADEDATE": {"type": "date", "bytes": 10, "max_size": 0},
    "SHORTNAME": {"type": "string", "bytes": 189, "max_size": 0},
    "SECID": {"type": "string", "bytes": 36, "max_size": 0},
    "NUMTRADES": {"type": "double"},
    "VALUE": {"type": "double"},
    "LOW": {"type": "double"},
    "HIGH": {"type": "double"},
    "CLOSE": {"type": "double"},
    "LEGALCLOSEPRICE": {"type": "double"},
    "ACCINT": {"type": "double"},
    "WAPRICE": {"type": "double"},
    "YIELDCLOSE": {"type": "double"},
    "OPEN": {"type": "double"},
    "VOLUME": {"type": "double"},
    "MARKETPRICE2": {"type": "double"},
    "MARKETPRICE3": {"type": "double"},
    "ADMITTEDQUOTE": {"type": "double"},
    "MP2VALTRD": {"type": "double"},
    "MARKETPRICE3TRADESVALUE": {"type": "double"},
    "ADMITTEDVALUE": {"type": "double"},
    "MATDATE": {"type": "date", "bytes": 10, "max_size": 0},
    "DURATION": {"type": "double"},
    "YIELDATWAP": {"type": "double"},
    "IRICPICLOSE": {"type": "double"},
    "BEICLOSE": {"type": "double"},
    "COUPONPERCENT": {"type": "double"},
    "COUPONVALUE": {"type": "double"},
    "BUYBACKDATE": {"type": "date", "bytes": 10, "max_size": 0},
    "LASTTRADEDATE": {"type": "date", "bytes": 10, "max_size": 0},
    "FACEVALUE": {"type": "double"},
    "CURRENCYID": {"type": "string", "bytes": 9, "max_size": 0},
    "CBRCLOSE": {"type": "double"},
    "YIELDTOOFFER": {"type": "double"},
    "YIELDLASTCOUPON": {"type": "double"},
    "OFFERDATE": {"type": "date", "bytes": 10, "max_size": 0},
    "FACEUNIT": {"type": "string", "bytes": 9, "max_size": 0},
    "TRADINGSESSION": {"type": "int32"}
}


class SecuritiesHistory:
    def __init__(self):
        self.metadata: dict[list[dict[str, dict]]] | None = None
        self.data: list[dict] | None = None
        self.class_name = self.__class__.__name__
        self.request_url: str = SHARES_HISTORY_REQUEST_URL
        self.db_table: str = ''
        self.db_table_columns: list[str] = []

    async def load_data_from_internet_async(self):

        columns = ("BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
                   "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3",
                   "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL",
                   "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR")


        # arguments = {'securities.columns': ('SECID', 'REGNUMBER', 'LOTSIZE', 'SHORTNAME')}
        arguments = {}
        async with aiohttp.ClientSession() as session:

            iss = aiomoex.ISSClient(session, self.request_url, query=arguments)
            data = await iss.get_all()  # iss.get()
            self.data = data['history']

            # self.data = await h_ex.get_history_all_securities_on_last_date(session,
            #                                                                columns=columns,
            #                                                                board="TQBR")  # board=None

            df = pd.DataFrame(self.data)
            df.set_index("SECID", inplace=True)
            print(df.head(10), "\n")
            print(df.tail(10), "\n")
            df.info()
            print(len(df), "\n")

            # if len(data) > 0:
            #     dbh.save_data_for_last_date(data, 'history.stock_shares_securities_history', 'TRADEDATE')

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.data)} {self.class_name} records loaded")