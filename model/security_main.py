import requests
import apimoex
import aiomoex
import aiohttp
from logger import Logger
import settings as st
import pandas as pd
import db_helper as dbh
from operator import itemgetter
from datetime import datetime

logger = Logger('security_main', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()

SHARES_MAIN_BOARD = "TQBR"
SHARES_MAIN_REQUEST_URL = (f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/'
                           f'{SHARES_MAIN_BOARD}/securities.json')


class SharesMain:
    def __init__(self):
        # self.metadata: dict[list[dict[str, dict]]] | None = None
        self.securities_data: list[dict] | None = None
        self.marketdata_data: list[dict] | None = None
        self.dataversion_data: list[dict] | None = None
        self.class_name = self.__class__.__name__
        self.request_url: str = SHARES_MAIN_REQUEST_URL
        # self.db_table: str = ''
        # self.stored_proc: str = ''
        # self.db_table_columns: list[str] = []

    async def load_data_from_internet_async(self):
        arguments = {}
        async with aiohttp.ClientSession() as session:
            iss = aiomoex.ISSClient(session, self.request_url, query=arguments)
            data = await iss.get()  # iss.get() get_all()
            self.securities_data = data['securities']
            self.marketdata_data = data['marketdata']
            self.dataversion_data = data['dataversion']

        df = pd.DataFrame(self.securities_data)
        df.set_index("SECID", inplace=True)
        print(df.head(10), "\n")
        print(df.tail(10), "\n")
        df.info()
        print(len(df), "\n")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.securities_data)} {self.class_name} securities records loaded")


        df = pd.DataFrame(self.marketdata_data)
        df.set_index("SECID", inplace=True)
        print(df.head(10), "\n")
        print(df.tail(10), "\n")
        df.info()
        print(len(df), "\n")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.marketdata_data)} {self.class_name} marketdata records loaded")

        df = pd.DataFrame(self.dataversion_data)
        print(df, "\n")
        df.info()
